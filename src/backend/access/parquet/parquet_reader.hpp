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
#ifndef PARQUET_READER__SDFES
#define PARQUET_READER__SDFES
#pragma once

#include "backend/sdb/common/pg_export.hpp"

#include <math.h>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include <arrow/api.h>
#include <parquet/arrow/reader.h>
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
  virtual arrow::Status Open() = 0;
  virtual arrow::Status Open(const char *dirname, Aws::S3::S3Client *s3_client) = 0;
  // virtual void Close() = 0;

  void SetRowgroupsList(const std::vector<int> &rowgroups);
  void SetOptions(bool use_threads, bool use_mmap);

  int32_t id();
};

ParquetReader *CreateParquetReader(Oid rel_id, uint64_t fileid,
							const char *filename, TupleDesc tuple_desc,
							const std::vector<bool> &fetched_col);


#endif
