#pragma once

#include "backend/sdb/common/pg_export.hpp"
#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include "backend/sdb/common/singleton.hpp"
#include "kvpair.pb.h"
#include "lake_service.pb.h"
#include "backend/sdb/common/common.hpp"

extern "C" {
#include "include/sdb/session_info.h"
}

/*
extern uint64_t read_xid;
extern uint64_t commit_xid;
extern uint64_t dbid;
extern uint64_t sessionid;
extern uint64_t query_id;
extern uint64_t slice_count;
extern uint64_t slice_seg_index;
*/

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

    std::unordered_map<uint64, sdb::RelFiles>::iterator it = rel_files_.find(rel);
    if (it != rel_files_.end()) {
      LOG(INFO) << "get rel files from local " << rel; 
      std::vector<sdb::LakeFile> files(it->second.files().begin(), it->second.files().end());
      for (size_t i = 0; i < files.size(); ++i) {
        LOG(INFO) << "file " << rel << " " << files[i].file_id(); 
      }
      return files;
    }

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

    if (thr_sess == nullptr) {
      // for test
	    create_session_context(TopMemoryContext);
    }

    auto& sess_info = thr_sess->session_cxt_;

    sdb::GetFilesRequest request;
    request.set_dbid(sess_info.dbid_);
    request.set_sessionid(sess_info.sessionid_);
    request.set_commandid(sess_info.query_id_);
    request.set_rel(rel);
    request.set_slice_count(sess_info.slice_count_);
    request.set_slice_seg_index(sess_info.slice_seg_index_);

    sdb::GetFilesResponse response;
    //request.set_message("I'm a RPC to connect stream");
    stub->GetFileList(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
      LOG(ERROR) << "Fail to GetFileList, database " << sess_info.dbid_ << " rel " << rel << " error" << cntl.ErrorText();
      return files;
    }
    for (int i = 0; i < response.files_size(); i++) {
      files.push_back(response.files(i)); 
      LOG(INFO) << "get a file: " << response.files(i).file_id();
    }
    return files;
  }

  void SetRelLakeLists(std::vector<sdb::RelFiles> rels) {
    for (size_t i = 0; i < rels.size(); ++i) {
      auto& rel = rels[i];
      rel_files_[rel.rel()] = rel;
      for (int j = 0; j < rel.files().size(); ++j) {
        LOG(INFO) << "file " << rel.rel() << " " << rel.files(j).file_id(); 
      }
    }
  }

private:
  std::unordered_map<uint64, sdb::RelFiles> rel_files_;
};

using LakeFileMgrSingleton = ThreadSafeSingleton<LakeFileMgr>;

}
