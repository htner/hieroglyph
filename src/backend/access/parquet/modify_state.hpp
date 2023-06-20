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

#include "backend/access/parquet/modify_reader.hpp"

extern "C"
{
#include "postgres.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
}

class ParquetS3ModifyState
{
private:
    /* list parquet reader of target files */
    std::shared_ptr<ModifyParquetReader> inserter;
    std::map<uint64_t, std::shared_ptr<ModifyParquetReader>> updates;
    std::list<std::shared_ptr<ModifyParquetReader>> uploads;
    /* memory context of reader */
    MemoryContext       cxt;
    /* target directory name */
    const char         *dirname;
    /* S3 system client */
    Aws::S3::S3Client  *s3_client;
    /* foreign table desc */
    TupleDesc           tuple_desc;
    /* parquet reader option */
    bool                use_threads;
    bool                use_mmap;
    /* schemaless mode flag */
    bool                schemaless;
    /* foreign table name */
    char               *rel_name;

    std::shared_ptr<arrow::Schema> file_schema_;

     /* list attnum of needed modify attributes */
    std::set<int>       target_attrs;

public:
    MemoryContext       fmstate_cxt;

public:
    ParquetS3ModifyState(MemoryContext reader_cxt,
                            const char *dirname,
                            Aws::S3::S3Client *s3_client,
                            TupleDesc tuple_desc,
                            std::set<int> target_attrs,
                            bool use_threads,
                            bool use_mmap);
    ~ParquetS3ModifyState();

    /* create reader for `filename` and add to list file */
    void add_file(uint64_t blockid, const char *filename);
    /* create new file and its temporary cache data */
    std::shared_ptr<ModifyParquetReader> new_inserter(const char *filename, TupleTableSlot *slot);
    /* execute insert `*slot` to cache data */
    bool exec_insert(TupleTableSlot *slot);
    /* execute update */
    bool exec_update(TupleTableSlot *slot, TupleTableSlot *planSlot);
    /* execute delete */
    bool exec_delete(ItemPointer tic);
    /* upload modified parquet file to storage system (local/S3) */
    void upload();
    /* true if s3_client is set */
    bool has_s3_client();

    /* create schema for new file */
    std::shared_ptr<arrow::Schema> create_new_file_schema(TupleTableSlot *slot);

    void set_rel_name(char *name);
};

ParquetS3ModifyState* create_parquet_modify_state(MemoryContext reader_cxt,
                                                     const char *dirname,
                                                     Aws::S3::S3Client *s3_client,
                                                     TupleDesc tuple_desc,
                                                     std::set<int> target_attrs,
                                                     bool use_threads,
                                                     bool use_mmap);

#endif
