/*-------------------------------------------------------------------------
 *
 * exec_state.hpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/exec_state.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARQUET_FDW_EXEC_STATE_HPP
#define PARQUET_FDW_EXEC_STATE_HPP

#include <list>
#include <set>

#include "backend/access/parquet/parquet_reader.hpp"

extern "C"
{
#include "postgres.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
}

enum ReaderType {
    RT_TRIVIAL = 0,
    RT_SINGLE,
    RT_MULTI,
    RT_MULTI_MERGE,
    RT_CACHING_MULTI_MERGE
};

class ParquetS3ReaderState {
public:
    virtual ~ParquetS3ReaderState() {};
    virtual void SetRel(Oid rel) {
      rel_ = rel;
    }
    virtual bool next(TupleTableSlot *slot, bool fake=false) = 0;
    virtual bool fetch(ItemPointer tid, TupleTableSlot *slot, bool fake=false) = 0;
    virtual void rescan(void) = 0;
    virtual void add_file(uint64_t fileid, const char *filename, List *rowgroups) = 0;
    virtual Size estimate_coord_size() = 0;
protected:
    Oid rel_;
};

ParquetS3ReaderState *create_parquet_execution_state(ReaderType reader_type,
                                                     MemoryContext reader_cxt,
                                                     const char *dirname,
                                                     Aws::S3::S3Client *s3_client,
                                                     Oid rel,
                                                     TupleDesc tuple_desc,
                                                     bool use_threads,
                                                     bool use_mmap,
                                                     int32_t max_open_files);


#endif
