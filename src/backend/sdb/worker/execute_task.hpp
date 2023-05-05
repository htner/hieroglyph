
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
    PrepareGuc();
    PrepareCatalog();
    HandleQuery();
  }

	void PrepareGuc() {
    // jump now
  }

	void PrepareCatalog() {
    // jump now
  }

	void HandleQuery() {
    CommandDest dest = whereToSendOutput;
    MemoryContext oldcontext;
    bool		save_log_statement_stats = log_statement_stats;
    bool		was_logged = false;
    char		msec_str[32];
    PlannedStmt	   *plan = NULL;
    QueryDispatchDesc *ddesc = NULL;
    CmdType		commandType = CMD_UNKNOWN;
    SliceTable *sliceTable = NULL;
    ExecSlice  *slice = NULL;
    ParamListInfo paramLI = NULL;
	}

private:
	const sdb::QueryRequest* request_;
	sdb::QueryReply* reply_;
  google::protobuf::Closure* done_;
};

using ExecuteTaskPtr = std::shared_ptr<ExecuteTask>;

}
