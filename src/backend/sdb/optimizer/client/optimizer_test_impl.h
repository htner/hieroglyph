#pragma once

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <brpc/restful.h>
#include "optimizer/optimizer.pb.h"

DECLARE_bool(gzip);

namespace sdb {

class OptimizerImpl : public sdb::Optimizer {
public:
    OptimizerImpl() = default;
	virtual ~OptimizerImpl() = default; 

    void Depart(google::protobuf::RpcController* cntl_base,
                 const sdb::OptimizeRequest* req,
                 sdb::OptimizeReply* res,
                 google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        if (FLAGS_gzip) {
            cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
        }
		std::string sql = req->sql();

		// List	   *parsetree_list;
		// ListCell   *parsetree_item;

		// List * pg_parse_query(const char *query_string)
        res->set_message("Hello " + req->name());
    }
};

}
