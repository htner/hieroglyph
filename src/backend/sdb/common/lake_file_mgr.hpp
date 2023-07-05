#pragma once

#include "backend/sdb/common/pg_export.hpp"
#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include "lake_service.pb.h"

extern uint64_t read_xid;
extern uint64_t commit_xid;
extern uint64_t dbid;
extern uint64_t sessionid;
extern uint64_t query_id;
extern uint64_t slice_count;
extern uint64_t slice_seg_index;

namespace sdb {
class LakeRelFiles {
public:
  LakeRelFiles() {

  }

private:
  //CommitVersion
  std::vector<sdb::LakeFile> files_;
};

class LakeFileMgr {
public:
  LakeFileMgr() {
  }

  std::vector<sdb::LakeFile> GetLakeFiles(uint64_t rel) {
    std::unique_ptr<brpc::Channel> channel;
    std::unique_ptr<sdb::Lake_Stub> stub;//(&channel);
    brpc::Controller cntl;
    channel = std::make_unique<brpc::Channel>();

    std::vector<sdb::LakeFile> files;
    //LOG(ERROR) << "prepare upload";
    // Initialize the channel, NULL means using default options. 
    brpc::ChannelOptions options;
    options.protocol = "h2:grpc";
    //options.connection_type = "pooled";
    options.timeout_ms = 10000/*milliseconds*/;
    options.max_retry = 5;
    if (channel->Init("127.0.0.1", 10001, &options) != 0) {
      LOG(ERROR) << "PrepareUpload: Fail to initialize channel";
      return files;
    }
    stub = std::make_unique<sdb::Lake_Stub>(channel.get());

    sdb::GetFilesRequest request;
    request.set_dbid(dbid);
    request.set_sessionid(sessionid);
    request.set_read_xid(read_xid);
    request.set_commit_xid(commit_xid);
    request.set_commandid(query_id);
    request.set_rel(rel);
    request.set_slice_count(slice_count);
    request.set_slice_seg_index(slice_seg_index);

    sdb::GetFilesResponse response;
    //request.set_message("I'm a RPC to connect stream");
    stub->GetFileList(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
      LOG(ERROR) << "Fail to PrepareInsertFiles, " << cntl.ErrorText();
      return files;
    }
    for (int i = 0; i < response.files_size(); i++) {
      files.push_back(response.files(i)); 
      LOG(INFO) << "get a file: " << response.files(i).file_name();
    }
    return files;
  }

private:
  //std::unordered_map<uint64, LakeRelFiles> rel_files_;
};

}
