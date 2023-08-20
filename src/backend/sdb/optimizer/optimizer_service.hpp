#pragma once

#include <thread>

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <brpc/restful.h>

#include "optimizer_service.pb.h"
#include "backend/sdb/common/singleton.hpp"
#include "backend/sdb/common/shared_queue.hpp"
#include "backend/sdb/optimizer/optimize_task.hpp"

DECLARE_bool(gzip);

namespace sdb {

using TaskQueueSingleton = ThreadSafeSingleton<SharedQueue<OptimizeTaskPtr> >;

class OptimizerService: public sdb::Optimizer {
public:
  OptimizerService() = default;
	virtual ~OptimizerService() = default; 

  void Optimize(google::protobuf::RpcController* cntl_base,
              const sdb::OptimizeRequest* req,
              sdb::OptimizeReply* res,
              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    LOG(ERROR) << "get one meesage";
    if (FLAGS_gzip) {
      cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
    }
    std::shared_ptr<OptimizeTask> task = std::make_shared<OptimizeTask>(req, res, done);
		TaskQueueSingleton::GetInstance()->push_back(task); 

    done_guard.release();
  }
};

}
