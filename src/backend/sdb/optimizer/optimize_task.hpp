#pragma once

#include <memory>

#include "optimizer/optimizer_service.pb.h"
#include "backend/sdb/common/pg_export.hpp"
#include "backend/sdb/optimizer/parser.hpp"

namespace sdb {

class OptimizeTask {
public:
	OptimizeTask(const sdb::OptimizeRequest* req, sdb::OptimizeReply* res, google::protobuf::Closure* done) :
    request_(req), reply_(res), done_(done) {
  }

	void Run() {
    PrepareCatalog();
	  std::unique_ptr<Parser> parser = std::make_unique<Parser>();
    List* parsetree_list = parser->Parse(request_->sql().data());
    HandleOptimize(parsetree_list);
  }

	void PrepareCatalog() {
  }

	void HandleOptimize(List* parsetree_list) {

		ListCell *parsetree_item;

		foreach (parsetree_item, parsetree_list) {
			RawStmt    *parsetree = lfirst_node(RawStmt, parsetree_item);
			List* querytree_list = pg_analyze_and_rewrite(parsetree, request_->sql().data(),
												NULL, 0, NULL);
			PlanQueries(querytree_list);
		}
	}

	void PlanQueries(List* querytree_list) {
		// List	   *stmt_list = NULL;
		ListCell   *query_list;
	  foreach (query_list, querytree_list) {	
      Query	   *query = lfirst_node(Query, query_list);
      // ConvertToPlanStmtFromDXL//
      PlanQuery(query);
    }
  }

  void PlanQuery(Query* query) {
    if (query->commandType == CMD_UTILITY) {
      return;     
    }
    optimize_query(query, CURSOR_OPT_PARALLEL_OK, NULL);
    // COptTasks::Optimize(query);
  } 

private:
	const sdb::OptimizeRequest* request_;
	sdb::OptimizeReply* reply_;
  google::protobuf::Closure* done_;
};

using OptimizeTaskPtr = std::shared_ptr<OptimizeTask>;

}
