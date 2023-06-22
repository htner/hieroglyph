/*-------------------------------------------------------------------------
 *
 * reader.hpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/reader.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARQUET_FDW_READER_HPP
#define PARQUET_FDW_READER_HPP

#include <math.h>

#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include "arrow/api.h"
#include "backend/access/parquet/parquet_s3/parquet_s3.hpp"
#include "parquet/arrow/reader.h"

extern "C" {
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "nodes/pg_list.h"
#include "parser/parse_oper.h"
#include "postgres.h"
#include "storage/spin.h"
#include "utils/jsonb.h"
#include "utils/sortsupport.h"
}

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <parquet/arrow/reader.h>

extern bool parquet_fdw_use_threads;

class ParallelCoordinator {
 private:
  enum Type { PC_SINGLE = 0, PC_MULTI };

  Type type;
  slock_t latch;
  union {
    struct {
      int32 reader;   /* current reader */
      int32 rowgroup; /* current rowgroup */
      int32 nfiles;   /* number of parquet files to read */
      int32 nrowgroups[FLEXIBLE_ARRAY_MEMBER]; /* per-file rowgroups numbers */
    } single; /* single file and simple multifile case */
    struct {
      int32 next_rowgroup[FLEXIBLE_ARRAY_MEMBER]; /* per-reader counters */
    } multi;                                      /* multimerge case */
  } data;

 public:
  void lock() { SpinLockAcquire(&latch); }
  void unlock() { SpinLockRelease(&latch); }

  void init_single(int32 *nrowgroups, int32 nfiles) {
    type = PC_SINGLE;
    data.single.reader = -1;
    data.single.rowgroup = -1;
    data.single.nfiles = nfiles;

    SpinLockInit(&latch);
    if (nfiles)
      memcpy(data.single.nrowgroups, nrowgroups, sizeof(int32) * nfiles);
  }

  void init_multi(int nfiles) {
    type = PC_MULTI;
    for (int i = 0; i < nfiles; ++i) data.multi.next_rowgroup[i] = 0;
  }

  /* Get the next reader id. Caller must hold the lock. */
  int32 next_reader() {
    if (type == PC_SINGLE) {
      /* Return current reader if it has more rowgroups to read */
      if (data.single.reader >= 0 && data.single.reader < data.single.nfiles &&
          data.single.nrowgroups[data.single.reader] > data.single.rowgroup + 1)
        return data.single.reader;

      data.single.reader++;
      data.single.rowgroup = -1;

      return data.single.reader;
    }

    Assert(false && "unsupported");
    return -1;
  }

  /* Get the next reader id. Caller must hold the lock. */
  int32 next_rowgroup(int32 reader_id) {
    if (type == PC_SINGLE) {
      if (reader_id != data.single.reader) return -1;
      return ++data.single.rowgroup;
    } else {
      return data.multi.next_rowgroup[reader_id]++;
    }

    Assert(false && "unsupported");
    return -1;
  }
};

class FastAllocatorS3;

enum ReadStatus { RS_SUCCESS = 0, RS_INACTIVE = 1, RS_EOF = 2 };

class ParquetReader {
 protected:
  std::string filename_;

  /* The reader identifier needed for parallel execution */
  int32_t reader_id_;

  std::unique_ptr<parquet::arrow::FileReader> reader_;

  std::vector<std::string> column_names_;

  /* Coordinator for parallel query execution */
  ParallelCoordinator *coordinator_;

  /*
   * List of row group indexes to scan
   */
  std::vector<int> rowgroups_;

  std::unique_ptr<FastAllocatorS3> allocator_;
  ReaderCacheEntry *reader_entry_;

  /*
   * libparquet options
   */
  bool use_threads_;
  bool use_mmap_;

  /* Whether object is properly initialized */
  bool initialized_;

 public:
  ParquetReader(MemoryContext cxt);
  virtual ~ParquetReader() = 0;
  virtual ReadStatus Next(TupleTableSlot *slot, bool fake = false) = 0;
  virtual void Rescan() = 0;
  virtual void Open() = 0;
  virtual void Open(const char *dirname, Aws::S3::S3Client *s3_client) = 0;
  // virtual void Close() = 0;

  void SetRowgroupsList(const std::vector<int> &rowgroups);
  void SetOptions(bool use_threads, bool use_mmap);
  void SetCoordinator(ParallelCoordinator *coord);

  int32_t id();
};

ParquetReader *CreateParquetReader(const char *filename, MemoryContext cxt,
                                   TupleDesc tuple_desc, int reader_id = -1,
                                   bool caching = false);

#endif
