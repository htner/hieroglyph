/*-------------------------------------------------------------------------
 *
 * modify_state.hpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2022, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/modify_state.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARQUET_FDW_MODIFY_STATE_HPP
#define PARQUET_FDW_MODIFY_STATE_HPP

#include <list>
#include <set>
#include <vector>

#include "backend/access/parquet/parquet_writer.hpp"

extern "C" {
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "postgres.h"
}

class ParquetS3ModifyState {
 private:
  /* list parquet reader of target files */
  std::shared_ptr<ParquetWriter> inserter_;
  std::map<uint64_t, std::shared_ptr<ParquetWriter>> updates;
  std::list<std::shared_ptr<ParquetWriter>> uploads_;
  /* memory context of reader */
  MemoryContext cxt;
  /* target directory name */
  const char *dirname;
  /* S3 system client */
  Aws::S3::S3Client *s3_client;
  /* foreign table desc */
  TupleDesc tuple_desc;
  /* parquet reader option */
  bool use_threads;
  bool use_mmap;
  /* schemaless mode flag */
  bool schemaless;
  /* foreign table name */
  char *rel_name;

  std::shared_ptr<arrow::Schema> file_schema_;

  /* list attnum of needed modify attributes */
  std::set<int> target_attrs;

 public:
  MemoryContext fmstate_cxt;

 public:
  ParquetS3ModifyState(MemoryContext reader_cxt, const char *dirname,
                       Aws::S3::S3Client *s3_client, TupleDesc tuple_desc,
                       std::set<int> target_attrs, bool use_threads,
                       bool use_mmap);
  ~ParquetS3ModifyState();

  /* create reader for `filename` and add to list file */
  // void add_file(uint64_t blockid, const char *filename, std);
  /* create new file and its temporary cache data */
  std::shared_ptr<ParquetWriter> new_inserter(const char *filename,
                                              TupleTableSlot *slot);
  /* execute insert `*slot` to cache data */
  bool ExecInsert(TupleTableSlot *slot);
  /* execute update */
  bool ExecUpdate(TupleTableSlot *slot, TupleTableSlot *planSlot);
  /* execute delete */
  bool ExecDelete(ItemPointer tic);
  /* upload modified parquet file to storage system (local/S3) */
  void upload();
  /* true if s3_client is set */
  bool HasS3Client();

  /* create schema for new file */
  std::shared_ptr<arrow::Schema> CreateNewFileSchema();

  void SetRelName(char *name);
};

ParquetS3ModifyState *create_parquet_modify_state(
    MemoryContext reader_cxt, const char *dirname, Aws::S3::S3Client *s3_client,
    TupleDesc tuple_desc, std::set<int> target_attrs, bool use_threads,
    bool use_mmap);

#endif
