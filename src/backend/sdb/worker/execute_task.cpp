#include <unistd.h>

#include "backend/sdb/worker/execute_task.hpp"
#include "backend/sdb/worker/motion_stream.hpp"
#include "backend/sdb/worker/worker_service.hpp"
#include "sdb/execute.h"

extern uint64_t read_xid;
extern uint64_t commit_xid;
extern uint64_t dbid;
extern uint64_t sessionid;
extern uint64_t query_id;
extern uint64_t slice_count;
extern uint64_t slice_seg_index;

namespace sdb {

ExecuteTask::ExecuteTask(const sdb::PrepareTaskRequest* req,
						 WorkerService* service) : request_(*req), service_(service) {
}

ExecuteTask::~ExecuteTask() {
}


void ExecuteTask::Run() {
	LOG(INFO) << "MPPEXEC: execute run";
	//kGlobalTask = shared_from_this();
	StartTransactionCommand();
	Prepare();
	PrepareGuc();
	PrepareCatalog();
	HandleQuery();
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
		if (route > 0 &&  motion_streams.size() >= (uint32_t)route) {
			motion_streams[route - 1]->SetStreamId(id);
			motion_streams[route - 1]->PushCache();
		}
	}
}

void ExecuteTask::Prepare() {
read_xid = request_.read_xid();
commit_xid = request_.commit_xid();
dbid = request_.dbid();
sessionid = request_.sessionid();
query_id = request_.task_identify().query_id();
}

void ExecuteTask::PrepareGuc() {
	// jump now
}

void ExecuteTask::PrepareCatalog() {
	// jump now
}

void ExecuteTask::HandleQuery() {
	/*
	 * Deserialize the query execution plan (a PlannedStmt node), if there is one.
	*/
	const std::string& plan_info = request_.plan_info();
	PlannedStmt* plan = (PlannedStmt *) deserializeNode(plan_info.data(), plan_info.size());
	if (!plan || !IsA(plan, PlannedStmt))
		LOG(ERROR) << "MPPEXEC: receive invalid planned statement";

	const std::string& plan_params = request_.plan_params();
	SerializedParams* params = (SerializedParams*) deserializeNode(plan_params.data(), plan_params.size());
	if (!params || !IsA(params, SerializedParams))
		LOG(ERROR) << "MPPEXEC: receive invalid params";

	auto slice_table = BuildSliceTable();
	std::string result_file = std::to_string(request_.task_identify().query_id()) + "_"
								+ std::to_string(request_.uid()) + "_"
								+ std::to_string(request_.dbid()) + ".queryres";

	set_worker_param(request_.sessionid(), request_.worker_id());
	exec_worker_query(request_.sql().data(), plan, params, slice_table,
					  request_.result_dir().data(), result_file.data(),
					  (void*)this);
}

SliceTable* ExecuteTask::BuildSliceTable() {
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
			slice_count = exec_slice->planNumSegments;
		}

		auto children = pb_exec_slice.children();
		for (int j = 0; j < children.size(); ++j) {
			exec_slice->children = lappend_int(exec_slice->children, children[j]);
		}

		auto segments = pb_exec_slice.segments();
		for (int j = 0; j < segments.size(); ++j) {
			exec_slice->segments = lappend_int(exec_slice->segments, segments[j]);
			if (segments[j] == table->localSegIndex) {
				slice_seg_index = j;
				LOG(ERROR) << "total seg num:" << slice_count << " my index:" << slice_seg_index; 

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
	if (recv_streams_.size() < motion_id) {
		recv_streams_.resize(count);
		mutexs_.clear();
		conds_.clear();
		for (int i = 0; i < count; ++i) {
			mutexs_.push_back(std::make_unique<std::mutex>());
			conds_.push_back(std::make_unique<std::condition_variable>());
		}
	}
	auto& streams = recv_streams_[motion_id - 1];
	if (streams.empty()) {
		for (int32_t i = 0; i < count; ++i) {
			auto stream = std::make_shared<MotionStream>(this);
			streams.push_back(std::move(stream));
		}
	}
	LOG(INFO) << "init recv_streams " << motion_id
		<< " | " << count << "|" << streams.size();
	// buffers_.Reset(count);
}

void ExecuteTask::InitSendStream(int32_t count) {
	if (send_streams_.empty()) {
		LOG(INFO) << " set send stream count to " << count;
		for (int32_t i = 0; i < count; ++i) {
			auto stream = std::make_shared<MotionStream>(this);
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
	if (recv_streams_.size() < motion_id) {
		// motion id must <= size
		LOG(INFO) << "motion id must <= size";
		return;
	}
	if (recv_streams_[motion_id - 1].size() <= from_route) {
		LOG(INFO) << "route must < size";
		return;
	}
	recv_streams_[motion_id - 1][from_route]->SetInfo(motion_id,
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
	if (send_streams_.size() <= to_route) {
		LOG(INFO) << " route must < size (" << send_streams_.size() << " < " << to_route << ")";
		return;
	}
	send_streams_[to_route]->SetInfo(motion_id,
								  from_slice,
								  to_slice,
								  from_segidex,
								  to_segidex,
								  from_route,
								  to_route);
}

bool ExecuteTask::StartSendStream(int32_t motion_id, int32_t route) {
	if (send_streams_.size() <= route) {
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
	if (send_streams_.size() > target_route) {
		// route must < size
		LOG(ERROR) << "send chunk to " << target_route << " data size" << message.size();
		send_streams_[target_route]->SendMessage(message.data(), message.size()); 
		return;
	}
	LOG(INFO) << "route must < size";
	LOG(ERROR) << "route" << target_route << " must not larger than "
	<< send_streams_.size();
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
	if (send_streams_.size() > target_route) {
		// route must < size
		send_streams_[target_route]->SendMessage(message.data(), message.size()); 
		return;
	}
	LOG(INFO) << "route must < size:" << send_streams_.size() << "|" << target_route;
}

const std::string& ExecuteTask::RecvTupleChunk(int32_t motion_id, int32 from_route) {

	cache_.clear();
	if (recv_streams_.size() < motion_id) {
		// motion_id must <= size
		LOG(ERROR) << "recv_stream.size() " << recv_streams_.size() << " less motion_id " << motion_id;
		return cache_;
	}
	if (recv_streams_[motion_id - 1].size() <= from_route) {
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
}

const std::string* ExecuteTask::RecvTupleChunkAny(int32_t motion_id, int32* target_route) {
	cache_.clear();
	if (recv_streams_.size() < motion_id) {
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
		for (int i = 0; i < streams.size(); ++i) {
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
}

void ExecuteTask::BroadcastStopMessage(int32_t motion_id) {

}

uint64_t ExecuteTask::GetQueryId() {
	return request_.task_identify().query_id();
}

MotionStream* ExecuteTask::GetRecvStream(int32_t motion_id, int32_t route) {
    if (motion_id > 0 && recv_streams_.size() >= (uint32_t)motion_id) {
      auto &motion_streams = recv_streams_[motion_id - 1];
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
	task->InitSendStream(num_segs);
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
}

bool SDBBroadcastChunk(void *t, 
				  TupleChunkListItem tc_item, 
				  int16 motion_id) {
	std::string_view msg((const char*)tc_item->chunk_data, tc_item->chunk_length);
	sdb::ExecuteTask* task = static_cast<sdb::ExecuteTask*>(t);
	task->BroadcastChunk(motion_id, msg);
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
