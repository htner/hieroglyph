/*-------------------------------------------------------------------------
 *
 * exec_state.cpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/exec_state.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "backend/sdb/common/pg_export.hpp"
#include "backend/access/parquet/reader_state.hpp"
#include "backend/access/parquet/heap.hpp"

#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include <sys/time.h>
#include <functional>
#include <list>


#if PG_VERSION_NUM < 110000
#define MakeTupleTableSlotCompat(tupleDesc) MakeSingleTupleTableSlot(tupleDesc)
#elif PG_VERSION_NUM < 120000
#define MakeTupleTableSlotCompat(tupleDesc) MakeTupleTableSlot(tupleDesc)
#else
#define MakeTupleTableSlotCompat(tupleDesc) MakeTupleTableSlot(tupleDesc, &TTSOpsVirtual)
#endif

/*
 * More compact form of common PG_TRY/PG_CATCH block which throws a c++
 * exception in case of errors.
 */
#define PG_TRY_INLINE(code_block, err) \
    do { \
        bool error = false; \
        PG_TRY(); \
        code_block \
        PG_CATCH(); { error = true; } \
        PG_END_TRY(); \
        if (error) { throw std::runtime_error(err); } \
    } while(0)


class TrivialExecutionStateS3 : public ParquetS3ReaderState
{
public:
    bool next(TupleTableSlot *, bool)
    {
        return false;
    }
    void rescan(void) {}
    void add_file(const char *, List *)
    {
        Assert(false && "add_file is not supported for TrivialExecutionStateS3");
    }
    void set_coordinator(ParallelCoordinator *) {}
    Size estimate_coord_size() 
    {
        Assert(false && "estimate_coord_size is not supported for TrivialExecutionStateS3");
		return 0;
    }
    void init_coord()
    {
        Assert(false && "init_coord is not supported for TrivialExecutionStateS3");
    }
};


class SingleFileExecutionStateS3 : public ParquetS3ReaderState {
private:
	ParquetReader      *reader;
	MemoryContext       cxt;
	ParallelCoordinator *coord;
	TupleDesc           tuple_desc;
	bool                use_mmap;
	bool                use_threads;
	std::string dirname;
	Aws::S3::S3Client *s3_client;

public:
    MemoryContext       estate_cxt;

    SingleFileExecutionStateS3(MemoryContext cxt,
                             const char *dirname,
                             Aws::S3::S3Client *s3_client,
                             TupleDesc tuple_desc,
                             bool use_threads,
                             bool use_mmap)
        : cxt(cxt), tuple_desc(tuple_desc),
          use_mmap(use_mmap), use_threads(use_threads),
          dirname(dirname), s3_client(s3_client)
    { }

    ~SingleFileExecutionStateS3()
    {
        if (reader)
            delete reader;
    }

    bool next(TupleTableSlot *slot, bool fake)
    {
        ReadStatus res;

        if ((res = reader->Next(slot, fake)) == RS_SUCCESS)
            ExecStoreVirtualTuple(slot);

        return res == RS_SUCCESS;
    }

    void rescan(void)
    {
        reader->Rescan();
    }

    void add_file(const char *filename, List *rowgroups)
    {
        ListCell           *lc;
        std::vector<int>    rg;

        foreach (lc, rowgroups)
            rg.push_back(lfirst_int(lc));

        reader = CreateParquetReader(rel_, filename, cxt, tuple_desc, -1);
        reader->SetOptions(use_threads, use_mmap);
        reader->SetRowgroupsList(rg);
        if (s3_client)
            reader->Open(dirname.c_str(), s3_client);
        else
            reader->Open();
    }

    void set_coordinator(ParallelCoordinator *coord)
    {
        this->coord = coord;

        if (reader)
            reader->SetCoordinator(coord);
    }

    Size estimate_coord_size() {
        return sizeof(ParallelCoordinator);
    }

    void init_coord() {
        coord->init_single(NULL, 0);
    }
};

class MultifileExecutionStateS3 : public ParquetS3ReaderState
{
private:
    struct FileRowgroups
    {
        std::string         filename;
        std::vector<int>    rowgroups;
    };
private:
    ParquetReader          *reader;

    std::vector<FileRowgroups> files;
    uint64_t                cur_reader;

    MemoryContext           cxt;
    TupleDesc               tuple_desc;
    bool                    use_threads;
    bool                    use_mmap;

    ParallelCoordinator    *coord;
		std::string dirname;
    Aws::S3::S3Client      *s3_client;

private:
    ParquetReader *get_next_reader()
    {
        ParquetReader *r;

        if (coord)
        {
            coord->lock();
            cur_reader = coord->next_reader();
            coord->unlock();
        }

        if (cur_reader >= files.size() || cur_reader < 0)
            return NULL;

        r = CreateParquetReader(rel_, files[cur_reader].filename.c_str(), cxt, tuple_desc, cur_reader);
        r->SetRowgroupsList(files[cur_reader].rowgroups);
        r->SetOptions(use_threads, use_mmap);
        r->SetCoordinator(coord);
        if (s3_client)
            r->Open(dirname.c_str(), s3_client);
        else
            r->Open();

        cur_reader++;

        return r;
    }

public:
    MultifileExecutionStateS3(MemoryContext cxt,
                            const char *dirname,
                            Aws::S3::S3Client *s3_client,
                            TupleDesc tuple_desc,
                            bool use_threads,
                            bool use_mmap)
        : reader(NULL), cur_reader(0), cxt(cxt), tuple_desc(tuple_desc),
          use_threads(use_threads), use_mmap(use_mmap),
          coord(NULL), dirname(dirname), s3_client(s3_client)
    { }

    ~MultifileExecutionStateS3()
    {
        if (reader)
            delete reader;
    }

    bool next(TupleTableSlot *slot, bool fake=false)
    {
        ReadStatus  res;

        if (unlikely(reader == NULL))
        {
            if ((reader = this->get_next_reader()) == NULL)
                return false;
        }

        res = reader->Next(slot, fake);

        /* Finished reading current reader? Proceed to the next one */
        if (unlikely(res != RS_SUCCESS)) {
            while (true) {
                if (reader) {
                    delete reader;
				}

                reader = this->get_next_reader();
                if (!reader)
                    return false;
                res = reader->Next(slot, fake);
                if (res == RS_SUCCESS) {
                    break;
				}
            }
        }

        if (res == RS_SUCCESS) {
            /*
             * ExecStoreVirtualTuple doesn't throw postgres exceptions thus no
             * need to wrap it into PG_TRY / PG_CATCH
             */
			// LOG(ERROR) << "get one";
            //ExecStoreVirtualTuple(slot);
			return true;
        }

        return false;
    }

    void rescan(void)
    {
        reader->Rescan();
    }

    void add_file(const char *filename, List *rowgroups)
    {
        FileRowgroups   fr;
        ListCell       *lc;

        fr.filename = filename;
        foreach (lc, rowgroups)
            fr.rowgroups.push_back(lfirst_int(lc));
        files.push_back(fr);
    }

    void set_coordinator(ParallelCoordinator *coord)
    {
        this->coord = coord;
    }

    Size estimate_coord_size()
    {
        return sizeof(ParallelCoordinator) + sizeof(int32) * files.size();
    }

    void init_coord()
    {
        ParallelCoordinator *coord = (ParallelCoordinator *) this->coord;
        int32  *nrowgroups;
        int     i = 0;

        nrowgroups = (int32 *) palloc(sizeof(int32) * files.size());
        for (auto &file : files)
            nrowgroups[i++] = file.rowgroups.size();
        coord->init_single(nrowgroups, files.size());
        pfree(nrowgroups);
    }
};

class MultifileMergeExecutionStateBaseS3 : public ParquetS3ReaderState
{
protected:
    struct ReaderSlot
    {
        int             reader_id;
        TupleTableSlot *slot;
    };

protected:
    std::vector<ParquetReader *> readers;

    MemoryContext       cxt;
    TupleDesc           tuple_desc;
    bool                use_threads;
    bool                use_mmap;
    ParallelCoordinator *coord;

    /*
     * Heap is used to store tuples in prioritized manner along with file
     * number. Priority is given to the tuples with minimal key. Once next
     * tuple is requested it is being taken from the top of the heap and a new
     * tuple from the same file is read and inserted back into the heap. Then
     * heap is rebuilt to sustain its properties. The idea is taken from
     * nodeGatherMerge.c in PostgreSQL but reimplemented using STL.
     */
    Heap<ReaderSlot>    slots;
    bool                slots_initialized;
	std::string dirname;
    Aws::S3::S3Client  *s3_client;
protected:
    /*
     * compare_slots
     *      Compares two slots according to sort keys. Returns true if a > b,
     *      false otherwise. The function is stolen from nodeGatherMerge.c
     *      (postgres) and adapted.
     */
    void set_coordinator(ParallelCoordinator *coord)
    {
        this->coord = coord;
        for (auto reader : readers)
            reader->SetCoordinator(coord);
    }

    Size estimate_coord_size()
    {
        return sizeof(ParallelCoordinator) + readers.size() * sizeof(int32);
    }

    void init_coord()
    {
        coord->init_multi(readers.size());
    }

};

ParquetS3ReaderState*
create_parquet_execution_state(ReaderType reader_type,
							   MemoryContext reader_cxt,
							   const char *dirname,
							   Aws::S3::S3Client *s3_client,
							   Oid rel,
							   TupleDesc tuple_desc,
							   bool use_threads,
							   bool use_mmap,
							   int32_t max_open_files)
{
	ParquetS3ReaderState* result;
    switch (reader_type)
    {
        case RT_TRIVIAL:
            result = new TrivialExecutionStateS3();
			break;
        case RT_SINGLE:
            result = new SingleFileExecutionStateS3(reader_cxt, dirname, s3_client, tuple_desc,
                                                         use_threads,
                                                         use_mmap);
			break;
        case RT_MULTI:
            result = new MultifileExecutionStateS3(reader_cxt, dirname, s3_client, tuple_desc,
                                                        use_threads,
                                                        use_mmap);
			break;
        default:
            throw std::runtime_error("unknown reader type");
    }
	result->SetRel(rel);
	return result;
}
