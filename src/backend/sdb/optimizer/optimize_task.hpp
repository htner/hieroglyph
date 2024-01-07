#pragma once

#include "backend/sdb/common/pg_export.hpp"

#include <memory>
#include <string_view>
#include <butil/logging.h>

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "optimizer_service.pb.h"

#include "backend/sdb/optimizer/parser.hpp"
#include "backend/sdb/common/common.hpp"
#include "backend/sdb/common/lake_file_mgr.hpp"
#include "backend/sdb/catalog_index/catalog_to_index.hpp"
// #include "utils/elog.h"

namespace sdb {

class OptimizeTask {
public:
	OptimizeTask(const sdb::OptimizeRequest* req, sdb::OptimizeReply* res, google::protobuf::Closure* done) :
    request_(req), reply_(res), done_(done) {
  }

  virtual ~OptimizeTask() {
      brpc::ClosureGuard done_guard(done_);
  }

	void Run(CatalogInfo &catalog_info);

	void PrepareCatalog(CatalogInfo &catalog_info);

	void HandleOptimize(List* parsetree_list);

	void PlanQueries(List* querytree_list);

  bool PlanQuery(Query* query);
  // FIXME_SDB call pg function now
  std::string PrepareParams(PlannedStmt* plannedstmt);

  void SetSlices(PlannedStmt* plan);

  void AddInsertRels(List *insert_list);

  void AddUpdateRels(List *update_list);

  void AddDeleteRels(List *delete_list);

  void AddReadRels(List *read_list);
 
private:
	const sdb::OptimizeRequest* request_;
	sdb::OptimizeReply* reply_;
  google::protobuf::Closure* done_;
};

using OptimizeTaskPtr = std::shared_ptr<OptimizeTask>;
}
