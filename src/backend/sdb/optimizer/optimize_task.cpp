#include "backend/sdb/optimizer/optimize_task.hpp"
#include "backend/sdb/common/s3_context.hpp"
#include "backend/sdb/common/log.hpp"

namespace sdb {

void OptimizeTask::Run(CatalogInfo &catalog_info) {
    auto& sess_info = thr_sess->session_cxt_;
    auto s3_cxt = GetS3Context();

	sess_info.dbid_ = request_->dbid();
	sess_info.sessionid_ = request_->sid();

	s3_cxt->lake_bucket_ = request_->db_space().s3_info().bucket();
	s3_cxt->lake_user_ = request_->db_space().s3_info().user();
	s3_cxt->lake_password_ = request_->db_space().s3_info().password();
	s3_cxt->lake_region_ = request_->db_space().s3_info().region();
	s3_cxt->lake_endpoint_ = request_->db_space().s3_info().endpoint();
	s3_cxt->lake_isminio_ = request_->db_space().s3_info().is_minio();
	/*
	kDBBucket = request_->db_space().s3_info().bucket();
	kDBS3User = request_->db_space().s3_info().user();
	kDBS3Password = request_->db_space().s3_info().password();
	kDBS3Region = request_->db_space().s3_info().region();
	kDBS3Endpoint = request_->db_space().s3_info().endpoint();
	kDBIsMinio = request_->db_space().s3_info().is_minio();
	*/
	reply_->set_rescode(0);

	whereToSendOutput = DestSDBCloud;

	StartTransactionCommand();
	PrepareCatalog(catalog_info);
	std::unique_ptr<Parser> parser = std::make_unique<Parser>();
	LOG(INFO) << "optimize sql:" << request_->sql();
	List* parsetree_list = parser->Parse(request_->sql().data());
	HandleOptimize(parsetree_list);
	CommitTransactionCommand();
}

void OptimizeTask::PrepareCatalog(CatalogInfo &catalog_info) {
	std::vector<sdb::RelFiles> catalog_list(request_->catalog_list().begin(), request_->catalog_list().end());
	LakeFileMgrSingleton::GetInstance()->SetRelLakeLists(catalog_list);

	Oid *oid_arr = nullptr;
	std::vector<Oid> oids;

	// now, we not support multi-session, so we should not consider reload diffrent version
	// catalog cunrrent.
	for (auto& rel_file : catalog_list) {
		uint64 oid = rel_file.rel();
		const std::string& rel_version = rel_file.version();
		if (sdb::reload_catalog_list.find(oid) != sdb::reload_catalog_list.end()) {
			std::lock_guard<std::mutex> lock(catalog_info.mtx_);
			std::string& cat_version = catalog_info.catalog_version[oid];
			if (cat_version != rel_version) {
				oids.push_back(oid);
			}
		}
	}

	if (!oids.empty()) {
		oid_arr = &oids[0];
		prepare_catalog(oid_arr, oids.size());
		// ResetCatalogCaches();
	}
}

void OptimizeTask::HandleOptimize(List* parsetree_list) {
	ListCell *parsetree_item;

	foreach (parsetree_item, parsetree_list) {
		RawStmt    *parsetree = lfirst_node(RawStmt, parsetree_item);
		List* querytree_list = pg_analyze_and_rewrite_with_error(parsetree, request_->sql().data(),
		NULL, 0, NULL);
		if (!querytree_list) {
			reply_->set_rescode(-1);
			// std::string err_msg(errinfo);
			auto& log_context = thr_sess->log_context_;
			auto message = reply_->mutable_message();
			if (log_context == nullptr) {
				message->set_code("58000");
				message->set_message("system error");
			} else {
				auto log_cxt = static_cast<LogDetail*>(thr_sess->log_context_);
				*message = log_cxt->LastErrorData();

				/*
				reply_->set_sql_err_code(last.sqlerrcode_);
				std::string errinfo = "message ";
				errinfo += last.message_;
				reply_->set_message(last.message_);
				*/
			}
			//LOG(ERROR) << err_msg; 
			return;
		}
		PlanQueries(querytree_list);
	}
}

void OptimizeTask::PlanQueries(List* querytree_list) {
	// List	   *stmt_list = NULL;
	ListCell   *query_list;
	foreach (query_list, querytree_list) {	
		// offset in the values builder.
		Query	   *query = lfirst_node(Query, query_list);
		// ConvertToPlanStmtFromDXL//
		PlanQuery(query);
	}
}

void OptimizeTask::PlanQuery(Query* query) {
	PlannedStmt *plan = NULL;
	char* plan_str = NULL;
	List* read_rel_list = NULL;
	List* insert_rel_list = NULL;
	List* update_rel_list = NULL;
	List* delete_rel_list = NULL;

	FetchRelationOidFromQuery(query, &read_rel_list, &insert_rel_list, &update_rel_list, &delete_rel_list);
	AddInsertRels(insert_rel_list);
	AddUpdateRels(update_rel_list);
	AddDeleteRels(delete_rel_list);
	AddReadRels(read_rel_list);

	if (query->commandType == CMD_UTILITY) {
		plan = utility_optimizer(query);
	} else {
		plan = orca_optimizer(query, CURSOR_OPT_PARALLEL_OK, NULL, &plan_str);
	}

	if (plan == NULL) {
		// reply_->set_message("plan is empty");
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
std::string OptimizeTask::PrepareParams(PlannedStmt* plannedstmt) {
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

void OptimizeTask::SetSlices(PlannedStmt* plan) {
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

void OptimizeTask::AddInsertRels(List *insert_list) {
	ListCell *lc = nullptr;

	if (insert_list == nullptr) {
		return;
	}

	foreach(lc, insert_list) {
		uint64 oid = lfirst_oid(lc);
		reply_->add_insert_rels(oid);
	}
}

void OptimizeTask::AddUpdateRels(List *update_list) {
	ListCell *lc = nullptr;

	if (update_list == nullptr) {
		return;
	}

	foreach(lc, update_list) {
		uint64 oid = lfirst_oid(lc);
		reply_->add_update_rels(oid);
	}
}

void OptimizeTask::AddDeleteRels(List *delete_list) {
	ListCell *lc = nullptr;

	if (delete_list == nullptr) {
		return;
}

	foreach(lc, delete_list) {
		uint64 oid = lfirst_oid(lc);
		reply_->add_delete_rels(oid);
	}
}

void OptimizeTask::AddReadRels(List *read_list) {
	ListCell *lc = nullptr;

	if (read_list == nullptr) {
		return;
	}

	foreach(lc, read_list) {
		uint64 oid = lfirst_oid(lc);
		reply_->add_read_rels(oid);
	}
}

}
