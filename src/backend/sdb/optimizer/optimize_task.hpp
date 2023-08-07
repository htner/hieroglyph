#pragma once

#include "backend/sdb/common/pg_export.hpp"

#include <memory>
#include <string_view>
#include <butil/logging.h>

#include "optimizer_service.pb.h"

#include "backend/sdb/optimizer/parser.hpp"
#include "backend/sdb/common/common.hpp"
#include "backend/sdb/common/lake_file_mgr.hpp"
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

	void Run() {
    kDBBucket = request_->db_space().base().bucket();
    kDBS3User = request_->db_space().detail().user();
    kDBS3Password = request_->db_space().detail().password();
    kDBS3Region = request_->db_space().detail().region();
    kDBS3Endpoint = request_->db_space().detail().endpoint();
    kDBIsMinio = request_->db_space().detail().is_minio();
    reply_->set_code(0);

    StartTransactionCommand();
	PrepareCatalog();
	  std::unique_ptr<Parser> parser = std::make_unique<Parser>();
    List* parsetree_list = parser->Parse(request_->sql().data());
    HandleOptimize(parsetree_list);
    CommitTransactionCommand();
  }

	void PrepareCatalog() {
    std::vector<sdb::RelFiles> catalog_list(request_->catalog_list().begin(), request_->catalog_list().end());
    LakeFileMgrSingleton::GetInstance()->SetRelLakeLists(catalog_list);

    Oid *oid_arr = nullptr;
    std::vector<Oid> oids;
    int size = request_->reload_catalog_oid().size();
    for (int i = 0; i < size; ++i) {
      oids.emplace_back(request_->reload_catalog_oid(i));
    }
    oid_arr = &oids[0];
	  prepare_catalog(oid_arr, size);
    // skip now
  }

	void HandleOptimize(List* parsetree_list) {
		ListCell *parsetree_item;

		foreach (parsetree_item, parsetree_list) {
			RawStmt    *parsetree = lfirst_node(RawStmt, parsetree_item);
			List* querytree_list = pg_analyze_and_rewrite(parsetree, request_->sql().data(),
												NULL, 0, NULL);
      if (!querytree_list) {
        std::string err_msg("analyze and rewrite failed");
        reply_->set_code(-1);
        reply_->set_message(err_msg);
        LOG(ERROR) << err_msg; 
        return;
      }
			PlanQueries(querytree_list);
		}
	}

	void PlanQueries(List* querytree_list) {
		// List	   *stmt_list = NULL;
		ListCell   *query_list;
	  foreach (query_list, querytree_list) {	
    // offset in the values builder.
      Query	   *query = lfirst_node(Query, query_list);
      // ConvertToPlanStmtFromDXL//
      PlanQuery(query);
    }
  }

  void PlanQuery(Query* query) {
    PlannedStmt *plan = NULL;
    char* plan_str = NULL;

    if (query->commandType == CMD_UTILITY) {
      plan = utility_optimizer(query);
    } else {
        plan = orca_optimizer(query, CURSOR_OPT_PARALLEL_OK, NULL, &plan_str);
    }

    if (plan == NULL) {
      reply_->set_message("plan is empty");
      return;
    }

    if (plan_str) {
      std::string plan_str_copy(plan_str);
      auto params_str = PrepareParams(plan);

      reply_->set_plan_dxl_str(plan_str_copy);
      reply_->set_plan_params_str(params_str);
      SetSlices(plan);
    }

    int planstmt_len;
    int planstmt_len_uncompressed;
    char* planstmt_cstr = serializeNode((Node*)plan, &planstmt_len, &planstmt_len_uncompressed);
    std::string planstmt_str(planstmt_cstr, planstmt_len);

    reply_->set_planstmt_str(planstmt_str);

    elog_node_display(PG_LOG, "query plan: ", plan, false);
  } 

  // FIXME_SDB call pg function now
  std::string PrepareParams(PlannedStmt* plannedstmt) {
    ParamExecData* exec_params = NULL;
    if (plannedstmt->paramExecTypes != NULL) {
      int param_exec = list_length(plannedstmt->paramExecTypes);
      exec_params = (ParamExecData*) palloc0(param_exec * sizeof(ParamExecData));
    }
    List* param_exec_types;
    ParamListInfo extern_params = NULL;
    Bitmapset* send_params = getExecParamsToDispatch(plannedstmt, exec_params, &param_exec_types);

    SerializedParams* sdb_serialized_params = serializeParamsForDispatch(extern_params,
                                                                         exec_params,
                                                                         param_exec_types,
                                                                         send_params);
    int params_len;
    int params_len_uncompressed;
    char* params = serializeNode((Node*)sdb_serialized_params, &params_len, &params_len_uncompressed);
    return std::string(params, params_len);
  }

  void SetSlices(PlannedStmt* plan) {
    for (int i = 0; i < plan->numSlices; ++i) {
      auto slice = plan->slices[i];
      PBPlanSlice* pb = reply_->add_slices();
      pb->set_slice_index(slice.sliceIndex);
      pb->set_parent_index(slice.parentIndex);
      pb->set_gang_type(slice.gangType);
      pb->set_num_segments(slice.numsegments);
      pb->set_segindex(slice.segindex);

      auto dispatch_info = pb->mutable_direct_dispatch_info();
      dispatch_info->set_is_direct_dispatch(slice.directDispatch.isDirectDispatch);

      ListCell   *lc;
      foreach(lc, slice.directDispatch.contentIds) {
        auto id = lfirst_int(lc);
        dispatch_info->add_segments(id);
      }
    }
  }
 
private:
	const sdb::OptimizeRequest* request_;
	sdb::OptimizeReply* reply_;
  google::protobuf::Closure* done_;
};

using OptimizeTaskPtr = std::shared_ptr<OptimizeTask>;
}
