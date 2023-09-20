#include "backend/sdb/common/threadlocal.h"
#include "include/sdb/session_info.h"
#include "include/utils/memutils.h"

THR_LOCAL ThreadSessionContext* thr_sess;

static void thread_task_init(SessionContext* context) {
	context->my_database_id_ = 0;
	context->my_database_table_space_ = 0;

	context->dbid_ = 0;
	context->sessionid_ = 0;
	context->query_id_ = 0;
	context->slice_count_ = 0;
	context->slice_seg_index_ = 0;

	context->max_concurrent_count_ = 0; 
	context->max_process_count_ = 0;
}

static void thread_session_init(ThreadSessionContext* sess) {
	thread_task_init(&sess->session_cxt_);
	InitS3Context(&sess->s3_context_);
	//cpp_thread_task_init(&sess->s3_context_)
}

ThreadSessionContext* create_session_context(MemoryContext parent) {
	ThreadSessionContext *sess;
	//ThreadSessionContext *old_sess = ;

	sess = (ThreadSessionContext*)MemoryContextAllocZero(parent, sizeof(ThreadSessionContext));

	MemoryContext top_mem_cxt;
	top_mem_cxt = AllocSetContextCreate(parent,
									 "SessionTopMemoryContext",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
	//sess->top_mem_cxt = top_mem_cxt;
	thread_session_init(sess);
	thr_sess = sess;
	return thr_sess;
}
