#pragma once

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <brpc/restful.h>

#include "worker/worker.pb.h"
#include "backend/sdb/common/singleton.hpp"
#include "backend/sdb/common/shared_queue.hpp"
#include "backend/sdb/worker/execute_task.hpp"

DECLARE_bool(gzip);

namespace sdb {

using ExecuteTaskQueueSingleton = ThreadSafeSingleton<SharedQueue<ExecuteTaskPtr> >;

class WorkerService: public sdb::Worker {
public:
  WorkerService() = default;
	virtual ~WorkerService() = default; 

    void Exec(google::protobuf::RpcController* cntl_base,
                 const sdb::QueryRequest* req,
                 sdb::QueryReply* res,
                 google::protobuf::Closure* done) {
      brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
      brpc::ClosureGuard done_guard(done);
      if (FLAGS_gzip) {
        cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
      }

      std::shared_ptr<ExecuteTask> task = std::make_shared<ExecuteTask>(req, res, done);
      ExecuteTaskQueueSingleton::GetInstance()->push_back(task); 
      done_guard.release();
    }
};

}
