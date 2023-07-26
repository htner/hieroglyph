#pragma once
#include "backend/sdb/common/pg_export.hpp"

#include <memory>
#include <string>
#include <vector>
#include <string_view>

#include <gflags/gflags.h>
#include <brpc/server.h>
#include <brpc/restful.h>
#include <butil/logging.h> // LOG Last

#include "include/sdb/execute.h"
#include "worker_service.pb.h"
#include "backend/sdb/common/shared_mutli_queue.hpp"
#include "backend/sdb/worker/motion_stream.hpp"

namespace sdb {

struct TaskIdentifyHash {
  std::size_t operator()(const TaskIdentify& t) const noexcept {
    std::size_t h1 = std::hash<uint64_t>{}(t.query_id());
    uint64_t high = t.slice_id();
    high = (high << 32 ) && t.seg_id();
    std::size_t h2 = std::hash<uint64_t>{}(high);
    return h1 ^ (h2 << 1); // or use boost::hash_combine
  }
};

struct TaskIdentifyEqual{
  bool operator()(const TaskIdentify& t1, const TaskIdentify& t2) const noexcept {
    return t1.query_id() == t2.query_id() && 
           t1.slice_id() == t2.slice_id() &&
            t1.seg_id() == t2.seg_id();
  }
};

class WorkerService;

class ExecuteTask : public std::enable_shared_from_this<ExecuteTask> {
public:
	ExecuteTask(const sdb::PrepareTaskRequest* req,
             WorkerService* service);

  virtual ~ExecuteTask();

	void Run();

  const TaskIdentify& GetKey();

  void StartRecvStream(int motion_id, int16 route, brpc::StreamId id);
  bool StartSendStream(int32_t motion_id, int32_t route);

	void Prepare();

	void PrepareGuc();

	void PrepareCatalog();

	void HandleQuery();

  SliceTable* BuildSliceTable();

  void InitRecvStream(int32_t motion_id, int32_t count);

  void InitSendStream(int32_t count);


  void SetupRecvStream(int32_t motion_id,
                       int32_t from_slice,
                       int32_t to_slice,
                       int32_t from_segidex,
                       int32_t to_segidex,
                       int32_t from_route,
                       int32_t to_route);

  void SetupSendStream(int32_t motion_id,
                       int32_t from_slice,
                       int32_t to_slice,
                       int32_t from_segidex,
                       int32_t to_segidex,
                       int32_t from_route,
                       int32_t to_route);

  void SendChunk(int32_t motion_id, int32 target_route, std::string_view& message);
  void BroadcastChunk(int32_t motion_id, std::string_view& message);

  void SendEos(int32_t motion_id, int32 target_route, std::string_view& message);

  const std::string& RecvTupleChunk(int32_t motion_id, int32 target_route);

  const std::string* RecvTupleChunkAny(int32_t motion_id, int32* target_route);

  void BroadcastStopMessage(int32_t motion_id);

  uint64_t GetQueryId();

  MotionStream* GetRecvStream(int32_t motion_id, int32_t route);

  void Lock(int32_t motion_id);
  void UnlockAndNotify(int32_t motion_id);

  void ReportResult(const std::string& result_dir, const std::string& result_file);

private:
  sdb::PrepareTaskRequest request_;
  std::vector<std::vector<std::shared_ptr<MotionStream>>> recv_streams_;
  std::vector<std::shared_ptr<MotionStream>> send_streams_;
  WorkerService* service_;
  //SharedMutliQueue<std::shared_ptr<butil::IOBuf> > buffers_;
  std::string cache_; // FIXME
  //
  std::vector<std::unique_ptr<std::mutex>> mutexs_;
  std::vector<std::unique_ptr<std::condition_variable>> conds_;
};

using ExecuteTaskPtr = std::shared_ptr<ExecuteTask>;

}
