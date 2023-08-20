#pragma once

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <brpc/restful.h>
#include <google/protobuf/text_format.h>

#include "worker_service.pb.h"
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

  void Prepare(google::protobuf::RpcController* cntl_base,
            const sdb::PrepareTaskRequest* req,
            sdb::PrepareTaskReply* res,
            google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    brpc::ClosureGuard done_guard(done);
    if (FLAGS_gzip) {
      cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
    }

    std::shared_ptr<ExecuteTask> task = std::make_shared<ExecuteTask>(req, this);
	  task->Prepare();

    std::unique_lock<std::mutex> mlock(mutex_);
    tasks_[task->GetKey()] = task;
    //done_guard.release();
  }

  void Start(google::protobuf::RpcController* cntl_base,
            const sdb::StartTaskRequest* req,
            sdb::StartTaskReply* res,
            google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    brpc::ClosureGuard done_guard(done);
    if (FLAGS_gzip) {
      cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP); 
    }

    std::unique_lock<std::mutex> mlock(mutex_);
    auto it = tasks_.find(req->task_identify());
    if (it == tasks_.end()) {
      return;
    }
    ExecuteTaskPtr ptr = it->second;
    mlock.unlock();     // unlock before notificiation to minimize mutex con
    
		LOG(ERROR) << "try start query task";
    ExecuteTaskQueueSingleton::GetInstance()->push_back(ptr); 
  }

  void StartStream(google::protobuf::RpcController* cntl_base,
                   const sdb::MotionStreamRequest* req,
                   sdb::MotionStreamReply* res,
                   google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    brpc::ClosureGuard done_guard(done);
    if (FLAGS_gzip) {
      cntl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
    }

    std::unique_lock<std::mutex> mlock(mutex_);
    auto it = tasks_.find(req->to_task_identify());
    if (it == tasks_.end()) {
      std::string debugstr;
      google::protobuf::TextFormat::PrintToString(req->to_task_identify(), &debugstr); //转换到字符串
      LOG(INFO) << "task not found:" << debugstr;
      cntl->SetFailed("Fail to accept stream, task not found");
      return;
    }
    mlock.unlock();     // unlock before notificiation to minimize mutex con

    brpc::StreamOptions stream_options;
    LOG(INFO) << "get recv stream:" << req->motion_id() << "|" << req->from_route();
    stream_options.handler = it->second->GetRecvStream(req->motion_id(),
                                                       req->from_route());
    if (stream_options.handler == nullptr) {
      // res->set_succ(false);
      cntl->SetFailed("Fail to accept stream ?? ");
      return;
    }
    brpc::StreamId id;
    if (brpc::StreamAccept(&id, *cntl, &stream_options) != 0) {
      cntl->SetFailed("Fail to accept stream ?? ");
      return;
    }
    LOG(INFO) << "start recv stream:" << id << "|" << req->from_route();
    it->second->StartRecvStream(req->motion_id(), req->from_route(), id);
    res->set_succ(true);
  }

  void FinishTask(const TaskIdentify& id) {
    std::unique_lock<std::mutex> mlock(mutex_);
    tasks_.erase(id);
  }

private:
  std::mutex mutex_;
  std::unordered_map<TaskIdentify, ExecuteTaskPtr, TaskIdentifyHash, TaskIdentifyEqual> tasks_; 
};

}
