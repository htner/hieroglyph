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
#include "backend/access/parquet/parquet_reader.hpp"

#include <list>

#include "arrow/api.h"
#include "arrow/array.h"
#include "arrow/io/api.h"
#include "backend/access/parquet/common.hpp"
#include "backend/new_executor/arrow/recordbatch_exchanger.hpp"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

extern "C" {
#include "access/sysattr.h"
#include "catalog/pg_collation_d.h"
#include "parser/parse_coerce.h"
#include "postgres.h"
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

#include "backend/sdb/common/pg_export.hpp"
#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <butil/logging.h>

#define SEGMENT_SIZE (1024 * 1024)

bool parquet_fdw_use_threads = true;

ParquetReader::ParquetReader() {
}

ParquetReader::~ParquetReader() {} 

int32_t ParquetReader::id() { return fileid_; }

void ParquetReader::SetRowgroupsList(const std::vector<int> &rowgroups) {
  rowgroups_ = rowgroups;
}

void ParquetReader::SetOptions(bool use_threads, bool use_mmap) {
  use_threads_ = use_threads;
  use_mmap_ = use_mmap;
}

class DefaultParquetReader : public ParquetReader {
 private:
  /* Current row group */
  std::shared_ptr<arrow::Table> table_;
  std::shared_ptr<pdb::RecordBatchExchanger> exchanger_;

  /*
   * Plain pointers to inner the structures of row group. It's needed to
   * prevent excessive shared_ptr management.
   */

  int row_group_;     /* current row group index */
  uint32_t num_rows_; /* total rows in row group */
  MemoryContext cxt_;

 public:
  /*
   * Constructor.
   * The fileid parameter is only used for parallel execution of
   * MultifileExecutionState.
   */
  DefaultParquetReader(Oid rel, uint64_t fileid, const char *filename,
                       TupleDesc tuple_desc)
      : ParquetReader(), row_group_(-1), num_rows_(0) {
    exchanger_ = std::make_shared<pdb::RecordBatchExchanger>(rel, tuple_desc);
    reader_entry_ = NULL;
    filename_ = filename;
    fileid_ = fileid;
    initialized_ = false;
  }

  ~DefaultParquetReader() {
    if (reader_entry_ && reader_entry_->file_reader && reader_)
      reader_entry_->file_reader->reader = std::move(reader_);
  }

  void Open(const char *dirname, Aws::S3::S3Client *s3_client) {
    // elog(WARNING, "parquet reader: open Parquet file on S3. %s%s", dname, fname);
    arrow::Status status;
    char *dname;
    char *fname;
    parquetSplitS3Path(dirname, filename_.c_str(), &dname, &fname);
    reader_entry_ = parquetGetFileReader(s3_client, dname, fname);
    //LOG(WARNING) <<  "parquet reader: open Parquet file on S3.  bucket: %s file:%s", dname, fname);
    pfree(dname);
    pfree(fname);

    reader_ = std::move(reader_entry_->file_reader->reader);

    /* Enable parallel columns decoding/decompression if needed */
    reader_->set_use_threads(use_threads_ && parquet_fdw_use_threads);
  }

  void Open() {
    arrow::Status status;
    std::unique_ptr<parquet::arrow::FileReader> reader;

    status = parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(),
        parquet::ParquetFileReader::OpenFile(filename_, use_mmap_), &reader);
    if (!status.ok())
      throw Error("parquet reader: failed to open Parquet file %s",
                  status.message().c_str());
    reader_ = std::move(reader);

    /* Enable parallel columns decoding/decompression if needed */
    reader_->set_use_threads(use_threads_ && parquet_fdw_use_threads);
  }

  bool ReadNextRowgroup() {
    arrow::Status status;
    row_group_++;

    /*
     * row_group cannot be less than zero at this point so it is safe to cast
     * it to unsigned int
     */
    if ((uint)row_group_ >= reader_->num_row_groups()) {
		LOG(ERROR) << "ReadNextRowgroup 3";
      return false;
    }

    int rowgroup = row_group_;
    auto rowgroup_meta =
        reader_->parquet_reader()->metadata()->RowGroup(rowgroup);

    status = reader_->RowGroup(rowgroup)->ReadTable(&table_);

    if (!status.ok()) {
		LOG(ERROR) << "ReadNextRowgroup 4";
      throw Error("parquet reader: failed to read rowgroup #%i: %s", rowgroup,
                  status.message().c_str());
	}

    if (!table_) {
		LOG(ERROR) << "ReadNextRowgroup 5";
		throw std::runtime_error("parquet reader: got empty table");
		}

    auto recordbatch = table_->CombineChunksToBatch();

    if (!recordbatch.status().ok()) {
		LOG(ERROR) << "ReadNextRowgroup 5";
      throw Error("parquet reader: failed to read rowgroup #%i: %s", rowgroup,
                  status.message().c_str());
	}
    LOG(ERROR) << "parquet reader size." << table_->num_rows() << ", "  << (*recordbatch)->num_rows();
    exchanger_->SetRecordBatch(*recordbatch);
    num_rows_ = table_->num_rows();

    return true;
  }

  ReadStatus Next(TupleTableSlot *slot, bool fake = false) {
    //LOG(ERROR) << " before fetch next tuple";
    auto result = exchanger_->FetchNextTuple();
    //LOG(ERROR) << " after fetch next tuple";
    while (!result.status().ok()) {
      /*
       * Read next row group. We do it in a loop to skip possibly empty
       * row groups.
       */
      do {
        if (!ReadNextRowgroup()) return RS_EOF;
      } while (!num_rows_);
	  // LOG(ERROR) << " before fetch next tuple 2";
      result = exchanger_->FetchNextTuple();
      //LOG(ERROR) << " after fetch next tuple 2";
	  if (result.status().ok()) {
		// ExecClearTuple(slot);
		break;
	  }
      // *slot = *result;
		
    }
	//LOG(ERROR) << "from attr: 1 -> "<< DatumGetUInt32((*result)->tts_values[0]);
	//LOG(ERROR) << "to attr: 1 -> "<< DatumGetUInt32(slot->tts_values[0]);
	ItemPointerSetBlockNumber(&((*result)->tts_tid), (uint32_t)fileid_);
	ExecCopySlot(slot, *result);
	ItemPointerCopy(&((*result)->tts_tid), &(slot->tts_tid));

//	LOG(WARNING) << "fetch next tuple, "
//	<< " tostring: " << ItemPointerToString(&(slot->tts_tid))
//	<< " tostring: " << ItemPointerToString(&((*result)->tts_tid))
//	<< " " << fileid_;
	//LOG(ERROR) << "parquet reader next " << fileid_ << ", ";
	//slot->
	//LOG(ERROR) << "from attr: 1 -> "<< DatumGetUInt32((*result)->tts_values[0]);
	//LOG(ERROR) << "to attr: 1 -> "<< DatumGetUInt32(slot->tts_values[0]);
    return RS_SUCCESS;
  }

  bool Fetch(uint32_t index, TupleTableSlot *slot) {
		// FIXME supoprt mutil row group
		arrow::Status status;
		int rowgroup = 0;
		auto rowgroup_meta =
			reader_->parquet_reader()->metadata()->RowGroup(rowgroup);

		status = reader_->RowGroup(rowgroup)->ReadTable(&table_);

		if (!status.ok()) {
			throw Error("parquet reader: failed to read rowgroup #%i: %s", rowgroup,
			   status.message().c_str());
		}

		if (!table_) {
			throw std::runtime_error("parquet reader: got empty table");
		}

		auto recordbatch = table_->CombineChunksToBatch();

		if (!recordbatch.status().ok()) {
			throw Error("parquet reader: failed to read rowgroup #%i: %s", rowgroup,
			   status.message().c_str());
		}
		LOG(ERROR) << "parquet reader fetch, size" << table_->num_rows() << ", "  << (*recordbatch)->num_rows();
		LOG(ERROR) << "parquet reader fetch, fileid " << fileid_ << ", "  << index;
		exchanger_->SetRecordBatch(*recordbatch);
		num_rows_ = table_->num_rows();
        auto result = exchanger_->FetchTuple(index);
		if (!result.status().ok()) {
			return false;
		}
		ItemPointerSetBlockNumber(&((*result)->tts_tid), (uint32_t)fileid_);
		ExecCopySlot(slot, *result);
		ItemPointerCopy(&(slot->tts_tid), &((*result)->tts_tid));
	//	ItemPointerSetBlockNumber(&(slot->tts_tid), (uint32_t)fileid_);
		return true;
   }

  void Rescan(void) {
    row_group_ = -1;
    num_rows_ = 0;
  }
};

ParquetReader *CreateParquetReader(Oid rel, uint64_t fileid, const char *filename,
                                   TupleDesc tuple_desc) {
  return new DefaultParquetReader(rel, fileid, filename, tuple_desc);
}
