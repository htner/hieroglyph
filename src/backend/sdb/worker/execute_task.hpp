
#pragma once
#include "backend/sdb/common/pg_export.hpp"

#include <memory>
#include <gflags/gflags.h>
#include <brpc/server.h>
#include <brpc/restful.h>
#include <butil/logging.h> // LOG Last

#include "worker/worker_service.pb.h"

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
    /*
     * Deserialize the query execution plan (a PlannedStmt node), if there is one.
    */
    const std::string& plan_info = request_->plan_info();
    PlannedStmt* plan = (PlannedStmt *) deserializeNode(plan_info.data(), plan_info.size());
    if (!plan || !IsA(plan, PlannedStmt))
      LOG(ERROR) << "MPPEXEC: receive invalid planned statement";

    const std::string& plan_params = request_->plan_params();
    SerializedParams* params = (SerializedParams*) deserializeNode(plan_params.data(), plan_params.size());
    if (!params || !IsA(params, SerializedParams))
      LOG(ERROR) << "MPPEXEC: receive invalid params";

    auto slice_table = BuildSliceTable();

    set_worker_param(request_->sessionid(), request_->worker_id());
    exec_worker_query(request_->sql().data(), plan, params, slice_table);
  }

  SliceTable* BuildSliceTable() {
    auto pb_table = request_->slice_table();

    auto table = makeNode(SliceTable);
    table->instrument_options = pb_table.instrument_options();
    table->hasMotions = pb_table.has_motions();
    table->localSlice = pb_table.local_slice();
    table->slices = (ExecSlice*)palloc0(sizeof(ExecSlice) * pb_table.slices_size());

    for (int i = 0; i < pb_table.slices_size(); ++i) {
        auto& pb_exec_slice = pb_table.slices()[i];
        ExecSlice* exec_slice = &table->slices[i];

        exec_slice->sliceIndex = pb_exec_slice.slice_index();
        exec_slice->planNumSegments = pb_exec_slice.plan_num_segments();
        auto segments = pb_exec_slice.segments();
        for (int j = 0; j < segments.size(); ++j) {
          exec_slice->segments = lappend_int(exec_slice->segments, segments[j]);
        }
        exec_slice->primaryGang = NULL;
        exec_slice->primaryProcesses = NULL;
        exec_slice->parentIndex = pb_exec_slice.parent_index();
        exec_slice->rootIndex = pb_exec_slice.root_index();
        exec_slice->gangType = (GangType)pb_exec_slice.gang_type();
    }
    return table;
  }

private:
  const sdb::QueryRequest* request_;
	sdb::QueryReply* reply_;
  google::protobuf::Closure* done_;
};

using ExecuteTaskPtr = std::shared_ptr<ExecuteTask>;

}
