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

enum ReadStatus { RS_SUCCESS = 0, RS_INACTIVE = 1, RS_EOF = 2 };

class ParquetReader {
 protected:
  std::string filename_;
  uint64_t fileid_;

  std::unique_ptr<parquet::arrow::FileReader> reader_;

  std::vector<std::string> column_names_;

  /*
   * List of row group indexes to scan
   */
  std::vector<int> rowgroups_;

  ReaderCacheEntry *reader_entry_;

  /*
   * libparquet options
   */
  bool use_threads_;
  bool use_mmap_;

  /* Whether object is properly initialized */
  bool initialized_;

 public:
  ParquetReader();
  virtual ~ParquetReader() = 0;
  virtual ReadStatus Next(TupleTableSlot *slot, bool fake = false) = 0;
  virtual bool Fetch(uint32_t index, TupleTableSlot *slot) = 0;
  virtual void Rescan() = 0;
  virtual void Open() = 0;
  virtual void Open(const char *dirname, Aws::S3::S3Client *s3_client) = 0;
  // virtual void Close() = 0;

  void SetRowgroupsList(const std::vector<int> &rowgroups);
  void SetOptions(bool use_threads, bool use_mmap);

  int32_t id();
};

ParquetReader *CreateParquetReader(Oid rel_id, uint64_t fileid, const char *filename,
                                   TupleDesc tuple_desc);

#endif
