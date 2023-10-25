#include "backend/sdb/common/pg_export.hpp"
#include "backend/sdb/common/s3_context.hpp"
#include "backend/sdb/common/flags.hpp"

extern "C" {
#include "include/sdb/session_info.h"
}

void thread_s3_context_init(void** thread_s3_context) {
	*thread_s3_context = NewObject(CurrentMemoryContext)S3Context();
}

S3Context* GetS3Context() {
	return static_cast<S3Context*>(thr_sess->s3_context_);
}

void InitS3Context(void** thread_s3_context) {
	thread_s3_context_init(thread_s3_context);

    auto s3_cxt = static_cast<S3Context*>(*thread_s3_context);

	s3_cxt->lake_bucket_ = FLAGS_bucket;
	s3_cxt->lake_user_ = FLAGS_s3user; 
	s3_cxt->lake_password_ = FLAGS_s3passwd; 
	s3_cxt->lake_region_ = FLAGS_region; 
	s3_cxt->lake_endpoint_ = FLAGS_endpoint;
	s3_cxt->lake_isminio_ = FLAGS_isminio;

	/*
	s3_cxt->result_bucket_ = FLAGS_s3bucket;
	s3_cxt->result_user_ = FLAGS_s3user;
	s3_cxt->result_password_ = FLAGS_s3password;
	s3_cxt->result_region_ = FLAGS_s3region;
	s3_cxt->result_endpoint_ = FLAGS_s3endpoint;
	s3_cxt->result_isminio_ = FLAGS_s3isminio;
	*/
}



