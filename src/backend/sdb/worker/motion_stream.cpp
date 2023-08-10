#include "backend/sdb/common/pg_export.hpp"
#include <butil/logging.h>

#include "backend/sdb/worker/motion_stream.hpp"
#include "backend/sdb/worker/execute_task.hpp"



namespace sdb {

MotionStream::MotionStream(ExecuteTask* t) : task_(t) {
}

void MotionStream::SetTask(ExecuteTask* t) {
	task_ = t;
}

void MotionStream::SetInfo(int32_t motion_id, 
               int32_t from_slice,
               int32_t to_slice,
               int32_t from_segindex,
               int32_t to_segindex,
               int32_t from_route,
               int32_t to_route) {
	motion_id_ = motion_id;
	from_slice_ = from_slice;
	to_slice_ = to_slice;
	from_segindex_ = from_segindex;
	to_segindex_ = to_segindex;
	from_route_ = from_route;
	to_route_ = to_route;
}

int MotionStream::on_received_messages(brpc::StreamId id, 
										   butil::IOBuf *const messages[], 
										   size_t size) {
	task_->Lock(motion_id_);
	for (size_t i = 0; i < size; ++i) {
		butil::IOBuf buf;
		buf.swap(*messages[i]);

		uint16_t tid;
		buf.copy_to((void*)&tid, 2, 2);

		read_buf_.push_back(std::move(buf));

		LOG(INFO) << "Received from Stream=" << id << "|" << buf.length() << "|" << tid;
		
	}
	task_->UnlockAndNotify(motion_id_);
	return 0;
}

void MotionStream::on_idle_timeout(brpc::StreamId id) {
	LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
}

void MotionStream::on_closed(brpc::StreamId id) {
	LOG(INFO) << "Stream=" << id << " is closed";
}

void MotionStream::SetStreamId(brpc::StreamId id) {
	LOG(INFO) << "set stream id " << id;
	stream_ = id;
}

bool MotionStream::Start(const sdb::TaskIdentify& key, const std::string& server_addr)  {
	if (stream_ != brpc::INVALID_STREAM_ID) {
		LOG(ERROR) << "Fail to initialize channel, initialize before";
		return false;
	}
	// A Channel represents a communication line to a Server. Notice that 
	// Channel is thread-safe and can be shared by all threads in your program.
	channel_ = std::make_unique<brpc::Channel>();

	// Initialize the channel, NULL means using default options. 
	brpc::ChannelOptions options;
	options.protocol = brpc::PROTOCOL_BAIDU_STD;
	options.connection_type = "pooled";
	options.timeout_ms = 10000/*milliseconds*/;
	options.max_retry = 5;
	if (channel_->Init(server_addr.c_str(), &options) != 0) {
		LOG(ERROR) << "Fail to initialize channel";
		return false;
	}

	// Normally, you should not call a Channel directly, but instead construct
	// a stub Service wrapping it. stub can be shared by all threads as well.
	stub_ = std::make_unique<sdb::Worker_Stub>(channel_.get());
	//cntl_ = std::make_unique<brpc::Controller>();
	if (brpc::StreamCreate(&stream_, cntl_, NULL) != 0) {
		LOG(ERROR) << "Fail to create stream";
		brpc::StreamClose(stream_);
		stream_ = brpc::INVALID_STREAM_ID;
		return false;
	}
	LOG(INFO) << "Created Stream=" << stream_ << " " << server_addr;
	sdb::MotionStreamRequest request;
	auto to_task = request.mutable_to_task_identify();
	//*to_task = key;
	to_task->set_query_id(key.query_id());
	to_task->set_slice_id(to_slice_);
	to_task->set_seg_id(to_segindex_);

	request.set_motion_id(motion_id_);
	request.set_from_slice(from_slice_);
	request.set_to_slice(to_slice_);
	request.set_from_segindex(from_segindex_);
	request.set_to_segindex(to_segindex_);
	request.set_from_route(from_route_);
	request.set_to_route(to_route_);
	sdb::MotionStreamReply response;
	//request.set_message("I'm a RPC to connect stream");
	stub_->StartStream(&cntl_, &request, &response, NULL);
	if (cntl_.Failed()) {
		brpc::StreamClose(stream_);
		stream_ = brpc::INVALID_STREAM_ID;
		LOG(ERROR) << "Fail to connect stream, " << cntl_.ErrorText();
		return false;
	}
	if (!response.succ()) {
		brpc::StreamClose(stream_);
		stream_ = brpc::INVALID_STREAM_ID;
		LOG(ERROR) << "Fail to connect stream";
	}
	return response.succ();
}

void MotionStream::SendMessage(const char* msg, size_t len) {
	butil::IOBuf buf;
	buf.append(msg, len);
	if (stream_ == brpc::INVALID_STREAM_ID) {
		LOG(INFO) << "write chunk to cache , len " << len;
		write_cache_.push_back(std::move(buf));
		return;
	}
	LOG(INFO) << "write chunk to stream " << stream_ << ", len " << len;
	CHECK_EQ(0, brpc::StreamWrite(stream_, buf));
}

void MotionStream::Close() {
	LOG(INFO) << "motion stream close";
	brpc::StreamClose(stream_);
	stream_ = brpc::INVALID_STREAM_ID;
}

void MotionStream::PushCache() {
	for (size_t i = 0; i < write_cache_.size(); ++i) {
		CHECK_EQ(0, brpc::StreamWrite(stream_, write_cache_[i]));
	}
	write_cache_.clear();
}


}
