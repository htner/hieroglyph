#pragma once
#include <string>
#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include "worker_service.pb.h"
//#include <butil/logging.h>

namespace sdb {

class ExecuteTask;

class MotionStream: public brpc::StreamInputHandler {
public:
  explicit MotionStream(ExecuteTask* t);

  void SetTask(ExecuteTask*);

  void SetInfo(int32_t motion_id, 
               int32_t from_slice,
               int32_t to_slice,
               int32_t from_segindex,
               int32_t to_segindex,
               int32_t from_route,
               int32_t to_route);

  virtual int on_received_messages(brpc::StreamId id, 
                                   butil::IOBuf *const messages[], 
                                   size_t size);

  virtual void on_idle_timeout(brpc::StreamId id);

  virtual void on_closed(brpc::StreamId id);

  void SetStreamId(brpc::StreamId id);

  bool Start(const sdb::TaskIdentify& key, const std::string& server_addr);

  void SendMessage(const char* msg, size_t len);

  void Close();

  void PushCache();

  bool TryGetBuf(butil::IOBuf* buf) {
    if (!read_buf_.empty()) {
      buf->swap(read_buf_.front());
      read_buf_.pop_front();
      return true;
    } 
    return false;
  }

  int32_t GetToSeg() {
    return to_segindex_;
  }

private:
  brpc::StreamId stream_ = brpc::INVALID_STREAM_ID;
  std::deque<butil::IOBuf> read_buf_;
  std::deque<butil::IOBuf> write_cache_;

  ExecuteTask* task_;

  std::unique_ptr<brpc::Channel> channel_;
  std::unique_ptr<sdb::Worker_Stub> stub_;//(&channel);
  brpc::Controller cntl_;

  int32_t motion_id_;
  int32_t from_slice_;
  int32_t to_slice_;
  int32_t from_segindex_;
  int32_t to_segindex_;
  int32_t from_route_;
  int32_t to_route_;
};

}
