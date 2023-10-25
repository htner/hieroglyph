#ifndef __INCLUDE_SESSION_INFO__H
#define __INCLUDE_SESSION_INFO__H

#include <stdint.h>

//#include "backend/sdb/common/pg_export.hpp"
#include "pg_config.h"
#include "c.h"
#include "postgres.h"
#include "include/sdb/threadlocal.h"

typedef struct SessionContext {
	Oid my_database_id_;
	Oid my_database_table_space_;
	uint64_t dbid_;
	uint64_t sessionid_;

	// for worker
	uint64_t query_id_;
	uint64_t slice_count_;
	uint64_t slice_seg_index_;

	// for worker/optimizer
	uint64_t max_concurrent_count_; 
	uint64_t max_process_count_;
} SessionContext;


typedef struct ThreadSessionContext {
	SessionContext session_cxt_;
	void *s3_context_;
	void *log_context_;
} ThreadSessionContext;

extern THR_LOCAL ThreadSessionContext* thr_sess;

extern ThreadSessionContext* create_session_context(MemoryContext parent);

extern void InitS3Context(void**);

extern void SendMessageToSession(ErrorData* edata);

#endif
