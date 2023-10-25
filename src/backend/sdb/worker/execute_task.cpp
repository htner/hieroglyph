#include <unistd.h>

#include "backend/sdb/worker/execute_task.hpp"
#include "backend/sdb/worker/motion_stream.hpp"
#include "backend/sdb/worker/worker_service.hpp"
#include "backend/sdb/common/common.hpp"
#include "backend/sdb/common/log.hpp"
#include "cdb/cdbvars.h"
#include "schedule_service.pb.h"
#include "sdb/execute.h"
#include "backend/sdb/common/lake_file_mgr.hpp"
#include "backend/sdb/common/s3_context.hpp"
#include "backend/sdb/catalog_index/catalog_to_index.hpp"

namespace sdb {

ExecuteTask::ExecuteTask(const sdb::PrepareTaskRequest* req,
						 WorkerService* service) : request_(*req), service_(service) {
}

ExecuteTask::~ExecuteTask() {
}


void ExecuteTask::Run(CatalogInfo& catalog_info) {
	LOG(INFO) << "MPPEXEC: execute run";

	//kGlobalTask = shared_from_this();
	// Prepare();
	InitThreadInfo();
	StartTransactionCommand();
    PrepareCatalog(catalog_info);
	PrepareGuc();
	HandleQuery();
	Upload();
	CommitTransactionCommand();

	service_->FinishTask(GetKey());
}

const TaskIdentify& ExecuteTask::GetKey() {
	return request_.task_identify();
}

void ExecuteTask::StartRecvStream(int motion_id, int16 route, brpc::StreamId id) {
	// send_streams_.push_back(std::move(stream));
	if (motion_id > 0 && recv_streams_.size() >= (uint32_t)motion_id) {
		auto &motion_streams = recv_streams_[motion_id - 1];
		if (route >= 0 &&  motion_streams.size() > (uint32_t)route) {
			motion_streams[route]->SetStreamId(id);
			// motion_streams[route - 1]->PushCache();
			return;
		}
	}
	LOG(ERROR) << "start recv_streams fail " << motion_id
		<< " | " << route;
}

void ExecuteTask::Prepare() {
}

void ExecuteTask::InitThreadInfo() {
    auto& sess_info = thr_sess->session_cxt_;
    auto s3_cxt = GetS3Context();

	sess_info.dbid_ = request_.dbid();
	sess_info.sessionid_ = request_.sessionid();
	sess_info.query_id_ = request_.task_identify().query_id();

	s3_cxt->lake_bucket_ = request_.db_space().s3_info().bucket();
	s3_cxt->lake_user_ = request_.db_space().s3_info().user();
	s3_cxt->lake_password_ = request_.db_space().s3_info().password();
	s3_cxt->lake_region_ = request_.db_space().s3_info().region();
	s3_cxt->lake_endpoint_ = request_.db_space().s3_info().endpoint();
	s3_cxt->lake_isminio_ = request_.db_space().s3_info().is_minio();

	s3_cxt->result_bucket_ = request_.result_space().s3_info().bucket();
	s3_cxt->result_user_ = request_.result_space().s3_info().user();
	s3_cxt->result_password_ = request_.result_space().s3_info().password();
	s3_cxt->result_region_ = request_.result_space().s3_info().region();
	s3_cxt->result_endpoint_ = request_.result_space().s3_info().endpoint();
	s3_cxt->result_isminio_ = request_.result_space().s3_info().is_minio();

	GpIdentity.dbid = sess_info.dbid_;

    std::vector<sdb::RelFiles> catalog_list(request_.catalog_list().begin(), request_.catalog_list().end());
    std::vector<sdb::RelFiles> user_rel_list(request_.user_rel_list().begin(), request_.user_rel_list().end());
	LakeFileMgrSingleton::GetInstance()->SetRelLakeLists(catalog_list);
	LakeFileMgrSingleton::GetInstance()->SetRelLakeLists(user_rel_list);

	// ResetCatalogCaches();
	// PrepareCatalog();
}

void ExecuteTask::PrepareGuc() {
	// jump now
}

void ExecuteTask::PrepareCatalog(CatalogInfo& catalog_info) {
    Oid *oid_arr = nullptr;
    std::vector<Oid> oids;
	for (auto& iter : request_.catalog_list()) {
      uint64 oid = iter.rel();
      const std::string& rel_version = iter.version();
      if (sdb::reload_catalog_list.find(oid) != sdb::reload_catalog_list.end()) {
        std::lock_guard<std::mutex> lock(catalog_info.mtx_);
        std::string& cat_version = catalog_info.catalog_version[oid];
        if (cat_version != rel_version) {
          oids.push_back(oid);
        }
      }
	}

    oid_arr = &oids[0];
	prepare_catalog(oid_arr, oids.size());
}

void ExecuteTask::HandleQuery() {
	/*
	 * Deserialize the query execution plan (a PlannedStmt node), if there is one.
	*/
    auto& sess_info = thr_sess->session_cxt_;

	const std::string& plan_info = request_.plan_info();
	PlannedStmt* plan = (PlannedStmt *) deserializeNode(plan_info.data(), plan_info.size());
	if (!plan || !IsA(plan, PlannedStmt)) {
		LOG(ERROR) << "MPPEXEC: receive invalid planned statement";
		std::unique_ptr<brpc::Channel> channel;
		std::unique_ptr<sdb::Schedule_Stub> stub;//(&channel);
		brpc::Controller cntl;
		channel = std::make_unique<brpc::Channel>();

		// Initialize the channel, NULL means using default options. 
		brpc::ChannelOptions options;
		options.protocol = "h2:grpc";
		//options.connection_type = "pooled";
		options.timeout_ms = 10000/*milliseconds*/;
		options.max_retry = 5;
		if (channel->Init("127.0.0.1", 10002, &options) != 0) {
			LOG(ERROR) << "Fail to initialize channel";
			return;
		}
		stub = std::make_unique<sdb::Schedule_Stub>(channel.get());

		sdb::PushWorkerResultRequest request;
		request.set_dbid(request_.dbid());
		request.set_sessionid(request_.sessionid());

		auto task_id = request.mutable_task_id();
		*task_id = request_.task_identify();
		auto result = request.mutable_result();
		result->set_dbid(request_.dbid());
		result->set_query_id(request_.task_identify().query_id());
		result->set_rescode(-1);
		//result->set_message("invalid planned statement");
		//
		auto message = result->mutable_message();
		message->set_code("50000");
		message->set_message("invalid planned statement");

		result->set_cmd_type(plan->commandType);
		//result->set_result_dir(result_dir);
		//result->set_meta_file("");
		//result->add_data_files(result_file);

		sdb::PushWorkerResultReply response;

		stub->PushWorkerResult(&cntl, &request, &response, NULL);
		if (cntl.Failed()) {
			LOG(ERROR) << "Fail to connect stream, " << cntl.ErrorText();
			return;
		}
		return;
	}
	
	SerializedParams *params = NULL;
	if (!request_.plan_params().empty()) {
		const std::string& plan_params = request_.plan_params();
		params = (SerializedParams*) deserializeNode(plan_params.data(), plan_params.size());
		if (!params || !IsA(params, SerializedParams))
			LOG(ERROR) << "MPPEXEC: receive invalid params";
	}

	SliceTable *slice_table = nullptr;

	if (plan->commandType != CMD_UTILITY) {
		slice_table = BuildSliceTable();
		GpIdentity.segindex = sess_info.slice_seg_index_;
	}

	std::string result_file = std::to_string(request_.task_identify().query_id()) + "_"
								+ std::to_string(request_.uid()) + "_"
								+ std::to_string(request_.dbid()) + ".queryres";

	set_worker_param(request_.sessionid(), request_.worker_id());

	uint64_t process_rows = 0;
	bool succ = exec_worker_query(request_.sql().data(), plan, params, slice_table,
					  request_.result_dir().data(), result_file.data(), 
					  &process_rows, (void*)this);
	if (succ) {
		ReportResult(plan->commandType, process_rows, request_.result_dir(), result_file);
	} else {
		std::unique_ptr<brpc::Channel> channel;
		std::unique_ptr<sdb::Schedule_Stub> stub;//(&channel);
		brpc::Controller cntl;
		channel = std::make_unique<brpc::Channel>();

		// Initialize the channel, NULL means using default options. 
		brpc::ChannelOptions options;
		options.protocol = "h2:grpc";
		//options.connection_type = "pooled";
		options.timeout_ms = 10000/*milliseconds*/;
		options.max_retry = 5;
		if (channel->Init("127.0.0.1", 10002, &options) != 0) {
			LOG(ERROR) << "Fail to initialize channel";
			return;
		}
		stub = std::make_unique<sdb::Schedule_Stub>(channel.get());

		auto log_cxt = static_cast<LogDetail*>(thr_sess->log_context_);
		//reply_->set_code(last.sqlerrorcode_);
		//reply_->set_message(last.message_);

		sdb::PushWorkerResultRequest request;
		request.set_dbid(request_.dbid());
		request.set_sessionid(request_.sessionid());

		auto task_id = request.mutable_task_id();
		*task_id = request_.task_identify();
		auto result = request.mutable_result();
		result->set_dbid(request_.dbid());
		result->set_query_id(request_.task_identify().query_id());
		result->set_rescode(-1);

		auto message = result->mutable_message();
		if (log_cxt) {
			*message = log_cxt->LastErrorData();
			// result->set_sql_err_code(last.sqlerrcode_);
			// result->set_message(last.message_);
		} else {
			//result->set_sql_err_code("58000");
			//result->set_message("invalid planned statement");
			message->set_code("58000");
			message->set_message("invalid planned statement");
		}
		result->set_cmd_type(plan->commandType);
		//result->set_result_dir(result_dir);
		//result->set_meta_file("");
		//result->add_data_files(result_file);

		sdb::PushWorkerResultReply response;

		stub->PushWorkerResult(&cntl, &request, &response, NULL);
		if (cntl.Failed()) {
			LOG(ERROR) << "Fail to connect stream, " << cntl.ErrorText();
			return;
		}
		return;

	}
}

void ExecuteTask::ReportResult(CmdType cmdtype, uint64_t process_rows,
	const std::string& result_dir, const std::string& result_file) {
	std::unique_ptr<brpc::Channel> channel;
	std::unique_ptr<sdb::Schedule_Stub> stub;//(&channel);
	brpc::Controller cntl;
	channel = std::make_unique<brpc::Channel>();

	// Initialize the channel, NULL means using default options. 
	brpc::ChannelOptions options;
	options.protocol = "h2:grpc";
	//options.connection_type = "pooled";
	options.timeout_ms = 10000/*milliseconds*/;
	options.max_retry = 5;
	if (channel->Init("127.0.0.1", 10002, &options) != 0) {
		LOG(ERROR) << "Fail to initialize channel";
		return;
	}
	stub = std::make_unique<sdb::Schedule_Stub>(channel.get());

	sdb::PushWorkerResultRequest request;
	request.set_dbid(request_.dbid());
	request.set_sessionid(request_.sessionid());

	auto task_id = request.mutable_task_id();
	*task_id = request_.task_identify();
	auto result = request.mutable_result();
	result->set_dbid(request_.dbid());
	result->set_query_id(request_.task_identify().query_id());
	result->set_rescode(0);
	result->set_result_dir(result_dir);

	result->set_cmd_type(cmdtype);

	result->set_meta_file("");
	result->add_data_files(result_file);

	sdb::PushWorkerResultReply response;

	stub->PushWorkerResult(&cntl, &request, &response, NULL);
	if (cntl.Failed()) {
		LOG(ERROR) << "Fail to connect stream, " << cntl.ErrorText();
		return;
	}
}

SliceTable* ExecuteTask::BuildSliceTable() {
    auto& sess_info = thr_sess->session_cxt_;

	if (!request_.has_slice_table()) {	
		LOG(INFO) << "not have slice table";
		return nullptr;
	}
	LOG(INFO) << "have slice table";
	auto pb_table = request_.slice_table();

	auto table = makeNode(SliceTable);
	table->instrument_options = pb_table.instrument_options();
	table->hasMotions = pb_table.has_motions();
	table->localSlice = pb_table.local_slice();
	table->localSegIndex = request_.task_identify().seg_id();
	table->slices = (ExecSlice*)palloc0(sizeof(ExecSlice) * pb_table.slices_size());
	table->numSlices = pb_table.slices_size();

	LOG(ERROR) << "table sizes" << pb_table.slices_size();
	LOG(ERROR) << "localSegindex " << table->localSegIndex; 
	LOG(ERROR) << "localSlice" << table->localSlice; 


	for (int i = 0; i < pb_table.slices_size(); ++i) {
		auto& pb_exec_slice = pb_table.slices()[i];
		ExecSlice* exec_slice = &table->slices[i];

		exec_slice->sliceIndex = pb_exec_slice.slice_index();
		exec_slice->planNumSegments = pb_exec_slice.plan_num_segments();

		if (i == table->localSlice) {
			// set global
			sess_info.slice_count_ = exec_slice->planNumSegments;
		}

		auto children = pb_exec_slice.children();
		for (int j = 0; j < children.size(); ++j) {
			exec_slice->children = lappend_int(exec_slice->children, children[j]);
		}

		auto segments = pb_exec_slice.segments();
		for (int j = 0; j < segments.size(); ++j) {
			exec_slice->segments = lappend_int(exec_slice->segments, segments[j]);
			if (segments[j] == table->localSegIndex) {
				sess_info.slice_seg_index_ = j;
				LOG(ERROR) << "total seg num:" << sess_info.slice_count_ << " my index:" << sess_info.slice_seg_index_; 

			}
		}

		exec_slice->primaryGang = NULL;
		//exec_slice->primaryProcesses = NULL;
		exec_slice->parentIndex = pb_exec_slice.parent_index();
		exec_slice->rootIndex = pb_exec_slice.root_index();
		exec_slice->gangType = (GangType)pb_exec_slice.gang_type();

		LOG(INFO) << "exec_slice:" << exec_slice->sliceIndex << " parentIndex:" << exec_slice->parentIndex
			<< " rootIndex:" << exec_slice->rootIndex << " leg:"<< list_length(exec_slice->segments);
	}
	return table;
}

void ExecuteTask::InitRecvStream(int32_t motion_id, int32_t count) {
	std::unique_lock<std::mutex> mlock(task_mutex_);
	if (recv_streams_.size() < (size_t)motion_id) {
		recv_streams_.resize(count);
		//mutexs_.clear();
		//conds_.clear();
		for (int i = mutexs_.size(); i < count; ++i) {
			mutexs_.push_back(std::make_unique<std::mutex>());
			conds_.push_back(std::make_unique<std::condition_variable>());
		}
	}
	auto& streams = recv_streams_[motion_id - 1];
	if (streams.empty()) {
		for (int32_t i = 0; i < count; ++i) {
			auto stream = std::make_shared<MotionStream>(this, motion_id);
			streams.push_back(std::move(stream));
		}
	} else {
		for (int32_t i = streams.size(); i < count; ++i) {
			auto stream = std::make_shared<MotionStream>(this, motion_id);
			streams.push_back(std::move(stream));
		}
	}
	LOG(INFO) << "init recv_streams " << motion_id
		<< " | " << count << "|" << streams.size();
	// buffers_.Reset(count);
}

void ExecuteTask::InitSendStream(int32_t motion_id, int32_t count) {
	if (send_streams_.empty()) {
		LOG(INFO) << " set send stream count to " << count;
		for (int32_t i = 0; i < count; ++i) {
			auto stream = std::make_shared<MotionStream>(this, motion_id);
			send_streams_.push_back(std::move(stream));
		}
	}
}

void ExecuteTask::SetupRecvStream(int32_t motion_id,
								  int32_t from_slice,
								  int32_t to_slice,
								  int32_t from_segidex,
								  int32_t to_segidex,
								  int32_t from_route,
								  int32_t to_route) {
	if (recv_streams_.size() < (size_t)motion_id) {
		// motion id must <= size
		LOG(INFO) << "motion id must <= size";
		return;
	}

	if (recv_streams_[motion_id - 1].size() <= (size_t)from_route) {
		LOG(INFO) << "route must < size";
		return;
	}

	recv_streams_[motion_id - 1][from_route]->SetInfo(
												  from_slice,
												  to_slice,
												  from_segidex,
												  to_segidex,
												  from_route,
												  to_route);
}

void ExecuteTask::SetupSendStream(int32_t motion_id,
								  int32_t from_slice,
								  int32_t to_slice,
								  int32_t from_segidex,
								  int32_t to_segidex,
								  int32_t from_route,
								  int32_t to_route) {
	if (send_streams_.size() <= (size_t)to_route) {
		LOG(INFO) << " route must < size (" << send_streams_.size() << " < " << to_route << ")";
		return;
	}
	send_streams_[to_route]->SetInfo(
								  from_slice,
								  to_slice,
								  from_segidex,
								  to_segidex,
								  from_route,
								  to_route);
}

bool ExecuteTask::StartSendStream(int32_t motion_id, int32_t route) {
	if (send_streams_.size() <= (size_t)route) {
		LOG(INFO) << "route must < size";
		return false;
	}
	auto& stream = send_streams_[route];
	auto workers = request_.workers();
	auto it = workers.find(stream->GetToSeg());
	if (it != workers.end()) {
		return send_streams_[route]->Start(request_.task_identify(), it->second.addr());
	}
	return false;
}

void ExecuteTask::SendChunk(int32_t motion_id, int32 target_route, std::string_view& message) {
	if (send_streams_.size() > (size_t)target_route) {
		// route must < size
		LOG(ERROR) << "send chunk to " << target_route << " data size" << message.size();
		send_streams_[target_route]->SendMessage(message.data(), message.size()); 
		return;
	}
	LOG(INFO) << "route must < size";
	LOG(ERROR) << "route" << target_route << " must not larger than "
	<< send_streams_.size();
	int* k= nullptr;
	*k = 1;
}

void ExecuteTask::BroadcastChunk(int32_t motion_id, std::string_view& message) {
	for (size_t i = 0; i < send_streams_.size(); ++i) {
		send_streams_[i]->SendMessage(message.data(), message.size()); 
	}
}


void ExecuteTask::SendEos(int32_t motion_id, int32 target_route, std::string_view& message) {
	if (target_route == -1) {
		BroadcastChunk(motion_id, message);
		return;
	}
	if (send_streams_.size() > (size_t)target_route) {
		// route must < size
		send_streams_[target_route]->SendMessage(message.data(), message.size()); 
		return;
	}
	LOG(INFO) << "route must < size:" << send_streams_.size() << "|" << target_route;
}

const std::string& ExecuteTask::RecvTupleChunk(int32_t motion_id, int32 from_route) {

	cache_.clear();
	if (recv_streams_.size() < (size_t)motion_id) {
		// motion_id must <= size
		LOG(ERROR) << "recv_stream.size() " << recv_streams_.size() << " less motion_id " << motion_id;
		return cache_;
	}
	if (recv_streams_[motion_id - 1].size() <= (size_t)from_route) {
		// route must < size
		LOG(ERROR) << "recv_stream.size() " << recv_streams_[motion_id - 1].size() 
			<< " less route" << from_route;
		return cache_;
	}
	auto& mutex = mutexs_[motion_id - 1];
	auto& cond = conds_[motion_id - 1];
	butil::IOBuf buf;
	bool succ = false;

	auto streams = recv_streams_[motion_id - 1];

	std::unique_lock<std::mutex> mlock(*mutex.get());
	while (!succ) {
		succ = streams[from_route]->TryGetBuf(&buf);
		if (succ) {
			auto str = buf.to_string();

			uint16_t tid;
			memcpy((void*)&tid, str.data() + 2, 2);
			LOG(ERROR) << "get one chunk from " << from_route << " str " << str.size()
				<< "|" << tid;
			cache_.swap(str);
			return cache_;
		}
		cond->wait(mlock);
	}
	return cache_;
}

const std::string* ExecuteTask::RecvTupleChunkAny(int32_t motion_id, int32* target_route) {
	cache_.clear();
	if (recv_streams_.size() < (size_t)motion_id) {
		LOG(ERROR) << "recv_stream.size() " << recv_streams_.size() << " less motion_id " << motion_id;
		return &cache_;
	}
	auto& mutex = mutexs_[motion_id - 1];
	auto& cond = conds_[motion_id - 1];
	butil::IOBuf buf;
	bool succ = false;
	auto streams = recv_streams_[motion_id - 1];

	std::unique_lock<std::mutex> mlock(*mutex.get());
	while (!succ) {
		for (size_t i = 0; i < streams.size(); ++i) {
			succ = streams[i]->TryGetBuf(&buf); 
			if (succ) {
				auto str = buf.to_string();

				uint16_t tid;
				memcpy((void*)&tid, str.data() + 2, 2);
		
				LOG(ERROR) << "get one chunk from " << i << " str " << str.size() 
					<< "|" << tid;
				cache_.swap(str);
				memcpy((void*)&tid, cache_.data() + 2, 2);
				LOG(ERROR) << "get one chunk from " << i << " str " << str.size() 
					<< "|" << tid;
				*target_route = i;
				return new std::string(cache_);	
				//return cache_;
			}
		}
		cond->wait(mlock);
	}
	return nullptr;
}

void ExecuteTask::BroadcastStopMessage(int32_t motion_id) {

}

uint64_t ExecuteTask::GetQueryId() {
	return request_.task_identify().query_id();
}

MotionStream* ExecuteTask::GetRecvStream(int32_t motion_id, int32_t route) {
	if (motion_id > 0 && recv_streams_.size() < (uint32_t)motion_id) {
		InitRecvStream(motion_id, route + 1);
	}

	if (motion_id > 0 && recv_streams_.size() >= (uint32_t)motion_id) {
		auto &motion_streams = recv_streams_[motion_id - 1];
		if (route >= 0 && motion_streams.size() <= (uint32_t)route) {
			// route as size, example route as 2, count as 2 too
			InitRecvStream(motion_id, route + 1);
		}

		if (route >= 0 && motion_streams.size() > (uint32_t)route) {
			return motion_streams[route].get();
		}
	}
	LOG(ERROR) << "get recv_streams fail " << motion_id
		<< " | " << route;
	return nullptr;
}

void ExecuteTask::Lock(int32_t motion_id) {
	auto& mutex = mutexs_[motion_id - 1];
	mutex->lock();
}

void ExecuteTask::UnlockAndNotify(int32_t motion_id) {
	auto& mutex = mutexs_[motion_id - 1];
	auto& cond = conds_[motion_id - 1];
	mutex->unlock();
	cond->notify_one();
}

} // namespace sdb

extern "C" {

void SDBInitRecvStream(void* t, int32 motion_id, int32 num_segs) {
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	task->InitRecvStream(motion_id, num_segs);
}

void SDBSetupRecvStream(void* t,
						int32 motion_id,
						int32_t from_slice,
						int32_t to_slice,
						int32_t from_segidex,
						int32_t to_segidex,
						int32_t from_route,
int32_t to_route) {
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	task->SetupRecvStream(motion_id,
					   from_slice,
					   to_slice,
					   from_segidex,
					   to_segidex,
					   from_route,
					   to_route);
}

void SDBInitSendStream(void* t, int32 motion_id, int32 num_segs) {
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	task->InitSendStream(motion_id, num_segs);
}

void SDBSetupSendStream(void* t,
						int32 motion_id,
						int32_t from_slice,
						int32_t to_slice,
						int32_t from_segidex,
						int32_t to_segidex,
						int32_t from_route,
int32_t to_route) {
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	task->SetupSendStream(motion_id,
					   from_slice,
					   to_slice,
					   from_segidex,
					   to_segidex,
					   from_route,
					   to_route);
}

bool SDBStartSendStream(void* t,
						int32 motion_id,
						int32 route) {
	for (int i = 0; i < 10; i++) {
		sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
		if (task->StartSendStream(motion_id, route)) {
			return true;
		}
		sleep(1);
	}
	return false;
}


bool SDBSendChunk(void *t, 
				  TupleChunkListItem tc_item, 
				  int16 motion_id,
				  int16 target_route) {
	std::string_view msg((const char*)tc_item->chunk_data, tc_item->chunk_length);
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	task->SendChunk(motion_id, target_route, msg);
	return true;
}

bool SDBBroadcastChunk(void *t, 
				  TupleChunkListItem tc_item, 
				  int16 motion_id) {
	std::string_view msg((const char*)tc_item->chunk_data, tc_item->chunk_length);
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	task->BroadcastChunk(motion_id, msg);
	return true;
}

TupleChunkListItem SDBRecvTupleChunkFrom(void *t, 
										 int16 motion_id, 
										 int16 source_route) {
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	auto buf = task->RecvTupleChunk(motion_id, source_route);

	auto tcItem = (TupleChunkListItem) palloc(sizeof(TupleChunkListItemData));
	tcItem->p_next = NULL;
	tcItem->chunk_length = buf.size();
	tcItem->inplace = (char*) (buf.data());

	//SetChunkDataSize(pData, 0);	
	return tcItem;
}

TupleChunkListItem SDBRecvTupleChunkFromAny(void *t,
											int16 motion_id,
											int16 *source_route) {
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	int32 r = 0;
	auto buf = task->RecvTupleChunkAny(motion_id, &r);
	*source_route = r;

	auto tcItem = (TupleChunkListItem) palloc(sizeof(TupleChunkListItemData));
	tcItem->p_next = NULL;
	tcItem->chunk_length = buf->size();
	tcItem->inplace = (char*) (buf->data());
	return tcItem;
}

void SDBBroadcastStopMessage(void* t,
						int32 motion_id) {
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	task->BroadcastStopMessage(motion_id);
}

void SDBSendEndOfStream(void *t,
						int motion_id,
						TupleChunkListItem tc_item) {
	std::string_view msg((const char*)tc_item->chunk_data, tc_item->chunk_length);
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	// FIXME
	task->SendEos(motion_id, -1, msg);
}

}
