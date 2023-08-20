
#pragma once
#include <memory>

#include "worker/worker.pb.h"
#include "backend/sdb/common/pg_export.hpp"

namespace sdb {

class ExecuteTask {
public:
	ExecuteTask(const sdb::QueryRequest* req, sdb::QueryReply* res, google::protobuf::Closure* done) :
    request_(req), reply_(res), done_(done) {
  }

	void Run() {
    PrepareCatalog();
    HandleQuery();
  }

	void PrepareCatalog() {
  }

	void HandleQuery() {
	}

private:
	const sdb::QueryRequest* request_;
	sdb::QueryReply* reply_;
  google::protobuf::Closure* done_;
};

using ExecuteTaskPtr = std::shared_ptr<ExecuteTask>;

}
