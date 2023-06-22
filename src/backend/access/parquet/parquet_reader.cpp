/*-------------------------------------------------------------------------
 *
 * reader.cpp
 *		  FDW routines for parquet reader
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet reader/src/reader.cpp
 *
 *-------------------------------------------------------------------------
 */
#include <list>

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/array.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

#include "backend/access/parquet/common.hpp"
#include "backend/access/parquet/parquet_reader.hpp"

#include "backend/new_executor/arrow/recordbatch_exchanger.hpp"

extern "C"
{
#include "postgres.h"
#include "access/sysattr.h"
#include "catalog/pg_collation_d.h"
#include "parser/parse_coerce.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#if PG_VERSION_NUM < 110000
#include "catalog/pg_type.h"
#else
#include "catalog/pg_type_d.h"
#endif
}

#define SEGMENT_SIZE (1024 * 1024)


bool parquet_fdw_use_threads = true;


class FastAllocatorS3 {
private:
    /*
     * Special memory segment to speed up bytea/Text allocations.
     */
    MemoryContext       segments_cxt;
    char               *segment_start_ptr;
    char               *segment_cur_ptr;
    char               *segment_last_ptr;
    std::list<char *>   garbage_segments;

public:
    FastAllocatorS3(MemoryContext cxt)
        : segments_cxt(cxt), segment_start_ptr(nullptr), segment_cur_ptr(nullptr),
          segment_last_ptr(nullptr), garbage_segments()
    {}

    ~FastAllocatorS3() {
        recycle();
    }

    /*
     * fast_alloc
     *      Preallocate a big memory segment and distribute blocks from it. When
     *      segment is exhausted it is added to garbage_segments list and freed
     *      on the next executor's iteration. If requested size is bigger that
     *      SEGMENT_SIZE then just palloc is used.
     */
    inline void *fast_alloc(long size) {
        void   *ret;

        Assert(size >= 0);

        /* If allocation is bigger than segment then just palloc */
        if (size > SEGMENT_SIZE) {
            MemoryContext oldcxt = MemoryContextSwitchTo(this->segments_cxt);
            void *block = exc_palloc(size);
            this->garbage_segments.push_back((char *) block);
            MemoryContextSwitchTo(oldcxt);

            return block;
        }

        size = MAXALIGN(size);

        /* If there is not enough space in current segment create a new one */
        if (this->segment_last_ptr - this->segment_cur_ptr < size) {
            MemoryContext oldcxt;

            /*
             * Recycle the last segment at the next iteration (if there
             * was one)
             */
            if (this->segment_start_ptr)
                this->garbage_segments.
                    push_back(this->segment_start_ptr);

            oldcxt = MemoryContextSwitchTo(this->segments_cxt);
            this->segment_start_ptr = (char *) exc_palloc(SEGMENT_SIZE);
            this->segment_cur_ptr = this->segment_start_ptr;
            this->segment_last_ptr =
                this->segment_start_ptr + SEGMENT_SIZE - 1;
            MemoryContextSwitchTo(oldcxt);
        }

        ret = (void *) this->segment_cur_ptr;
        this->segment_cur_ptr += size;

        return ret;
    }

    void recycle(void)
    {
        /* recycle old segments if any */
        if (!this->garbage_segments.empty())
        {
            bool    error = false;

            PG_TRY();
            {
                for (auto it : this->garbage_segments)
                    pfree(it);
            }
            PG_CATCH();
            {
                error = true;
            }
            PG_END_TRY();
            if (error)
                throw std::runtime_error("garbage segments recycle failed");

            this->garbage_segments.clear();
            elog(DEBUG1, "parquet reader: garbage segments recycled");
        }
    }

    MemoryContext context()
    {
        return segments_cxt;
    }
};


ParquetReader::ParquetReader(MemoryContext cxt)
    : allocator_(new FastAllocatorS3(cxt))
{}

int32_t ParquetReader::id() {
    return reader_id_;
}

void ParquetReader::SetRowgroupsList(const std::vector<int> &rowgroups) {
    rowgroups_ = rowgroups;
}

void ParquetReader::SetOptions(bool use_threads, bool use_mmap) {
    use_threads_ = use_threads;
    use_mmap_ = use_mmap;
}

void ParquetReader::SetCoordinator(ParallelCoordinator *coord) {
    coordinator_ = coord;
}

class DefaultParquetReader : public ParquetReader {
private:
   /* Current row group */
    std::shared_ptr<arrow::Table>   table_;
	std::shared_ptr<pdb::RecordBatchExchanger>  exchanger_;

    /*
     * Plain pointers to inner the structures of row group. It's needed to
     * prevent excessive shared_ptr management.
     */

    int             row_group_;          /* current row group index */
    uint32_t        num_rows_;           /* total rows in row group */

public:
    /* 
     * Constructor.
     * The reader_id parameter is only used for parallel execution of
     * MultifileExecutionState.
     */
    DefaultParquetReader(const char* filename, MemoryContext cxt, TupleDesc tuple_desc, int reader_id = -1)
        : ParquetReader(cxt), row_group_(-1), num_rows_(0) {
		exchanger_ = std::make_shared<pdb::RecordBatchExchanger>(tuple_desc);
        reader_entry_ = NULL;
        filename_ = filename;
        reader_id_ = reader_id;
        coordinator_ = NULL;
        initialized_ = false;
    }

    ~DefaultParquetReader() {
        if (reader_entry_ && reader_entry_->file_reader && reader_)
            reader_entry_->file_reader->reader = std::move(reader_);
    }

    void Open(const char *dirname,
              Aws::S3::S3Client *s3_client) {
        arrow::Status   status;
        char *dname;
        char *fname;
        parquetSplitS3Path(dirname, filename_.c_str(), &dname, &fname);
        reader_entry_ = parquetGetFileReader(s3_client, dname, fname);
        elog(DEBUG1, "parquet reader: open Parquet file on S3. %s%s", dname, fname);
        pfree(dname);
        pfree(fname);

        reader_ = std::move(reader_entry_->file_reader->reader);

        /* Enable parallel columns decoding/decompression if needed */
        reader_->set_use_threads(use_threads_ && parquet_fdw_use_threads);
    }

    void Open() {
        arrow::Status   status;
        std::unique_ptr<parquet::arrow::FileReader> reader;

        status = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename_, use_mmap_),
                        &reader);
        if (!status.ok())
            throw Error("parquet reader: failed to open Parquet file %s",
                                 status.message().c_str());
        reader_ = std::move(reader);

        /* Enable parallel columns decoding/decompression if needed */
        reader_->set_use_threads(use_threads_ && parquet_fdw_use_threads);
    }

    bool ReadNextRowgroup() {
        arrow::Status               status;

        /*
         * In case of parallel query get the row group index from the
         * coordinator. Otherwise just increment it.
         */
        if (coordinator_) {
            coordinator_->lock();
            if ((row_group_ = coordinator_->next_rowgroup(reader_id_)) == -1) {
                coordinator_->unlock();
                return false;
            }
            coordinator_->unlock();
        } else {
            row_group_++;
		}

        /*
         * row_group cannot be less than zero at this point so it is safe to cast
         * it to unsigned int
         */
        if ((uint) row_group_ >= rowgroups_.size()) {
            return false;
		}

        int  rowgroup = rowgroups_[row_group_];
        auto rowgroup_meta = reader_->parquet_reader()
									->metadata()
									->RowGroup(rowgroup);

        status = reader_->RowGroup(rowgroup)
			->ReadTable(&table_);

        if (!status.ok())
            throw Error("parquet reader: failed to read rowgroup #%i: %s",
                        rowgroup, status.message().c_str());

        if (!table_)
            throw std::runtime_error("parquet reader: got empty table");


		auto recordbatch = table_->CombineChunksToBatch();

        if (!recordbatch.status().ok())
            throw Error("parquet reader: failed to read rowgroup #%i: %s",
                        rowgroup, status.message().c_str());
		exchanger_->SetRecordBatch(*recordbatch);

        num_rows_ = table_->num_rows();

        return true;
    }

    ReadStatus Next(TupleTableSlot *slot, bool fake=false)
    {
        allocator_->recycle();

		auto result = exchanger_->FetchNextTuple();
		if(!result.status().ok()) {
            /*
             * Read next row group. We do it in a loop to skip possibly empty
             * row groups.
             */
            do {
                if (!ReadNextRowgroup())
                    return RS_EOF;
            } while (!num_rows_);
			result = exchanger_->FetchNextTuple();
			//*slot = *result;
        }
        return RS_SUCCESS;
    }

    

    void Rescan(void) {
        row_group_ = -1;
        num_rows_ = 0;
    }
};

ParquetReader *CreateParquetReader(const char *filename,
                                     MemoryContext cxt,
									TupleDesc tuple_desc,
                                     int reader_id,
                                     bool caching) {
    return new DefaultParquetReader(filename, cxt, tuple_desc, reader_id);
}


/* Default destructor is required */
ParquetReader::~ParquetReader() {}
