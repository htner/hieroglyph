
#include "backend/sdb/common/pg_export.hpp"

#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include "lake_service.pb.h"

extern uint64_t commit_xid;

extern "C"
bool SDB_StartTransaction(uint64 dbid, uint64 sid) {
	std::unique_ptr<brpc::Channel> channel;
	std::unique_ptr<sdb::Lake_Stub> stub;//(&channel);
	brpc::Controller cntl;
	channel = std::make_unique<brpc::Channel>();

	LOG(ERROR) << "prepare upload";
	// Initialize the channel, NULL means using default options. 
	brpc::ChannelOptions options;
	options.protocol = "h2:grpc";
	//options.connection_type = "pooled";
	options.timeout_ms = 10000/*milliseconds*/;
	options.max_retry = 5;
	if (channel->Init("127.0.0.1", 10001, &options) != 0) {
		LOG(ERROR) << "PrepareUpload: Fail to initialize channel";
		return false;
	}
	stub = std::make_unique<sdb::Lake_Stub>(channel.get());

	sdb::StartTransactionRequest request;
	request.set_dbid(dbid);
	request.set_sessionid(sid);

	sdb::StartTransactionResponse response;
	//request.set_message("I'm a RPC to connect stream");
	stub->Start(&cntl, &request, &response, NULL);
	if (cntl.Failed()) {
		LOG(ERROR) << "Fail to PrepareInsertFiles, " << cntl.ErrorText();
		return false;
	}
	return true;
}

extern "C"
bool SDB_CommitTransaction(uint64 dbid, uint64 sid) {
	std::unique_ptr<brpc::Channel> channel;
	std::unique_ptr<sdb::Lake_Stub> stub;//(&channel);
	brpc::Controller cntl;
	channel = std::make_unique<brpc::Channel>();

	LOG(ERROR) << "prepare upload";
	// Initialize the channel, NULL means using default options. 
	brpc::ChannelOptions options;
	options.protocol = "h2:grpc";
	//options.connection_type = "pooled";
	options.timeout_ms = 10000/*milliseconds*/;
	options.max_retry = 5;
	if (channel->Init("127.0.0.1", 10001, &options) != 0) {
		LOG(ERROR) << "PrepareUpload: Fail to initialize channel";
		return false;
	}
	stub = std::make_unique<sdb::Lake_Stub>(channel.get());

	sdb::CommitRequest request;
	request.set_dbid(dbid);
	request.set_sessionid(sid);

	sdb::CommitResponse response;
	//request.set_message("I'm a RPC to connect stream");
	stub->Commit(&cntl, &request, &response, NULL);
	if (cntl.Failed()) {
		LOG(ERROR) << "Fail to PrepareInsertFiles, " << cntl.ErrorText();
		return false;
	}
	return true;
}
extern "C"
bool SDB_AbortTransaction(uint64 dbid, uint64 sid) {
	std::unique_ptr<brpc::Channel> channel;
	std::unique_ptr<sdb::Lake_Stub> stub;//(&channel);
	brpc::Controller cntl;
	channel = std::make_unique<brpc::Channel>();

	LOG(ERROR) << "prepare upload";
	// Initialize the channel, NULL means using default options. 
	brpc::ChannelOptions options;
	options.protocol = "h2:grpc";
	//options.connection_type = "pooled";
	options.timeout_ms = 10000/*milliseconds*/;
	options.max_retry = 5;
	if (channel->Init("127.0.0.1", 10001, &options) != 0) {
		LOG(ERROR) << "PrepareUpload: Fail to initialize channel";
		return false;
	}
	stub = std::make_unique<sdb::Lake_Stub>(channel.get());

	sdb::AbortRequest request;
	request.set_dbid(dbid);
	request.set_sessionid(sid);

	sdb::AbortResponse response;
	//request.set_message("I'm a RPC to connect stream");
	stub->Abort(&cntl, &request, &response, NULL);
	if (cntl.Failed()) {
		LOG(ERROR) << "Fail to PrepareInsertFiles, " << cntl.ErrorText();
		return false;
	}
	return true;
}

extern "C"
bool SDB_AlloccateXid(uint64 dbid, uint64 sid, bool read, bool write, uint64* read_xid, uint64* write_xid) {
	std::unique_ptr<brpc::Channel> channel;
	std::unique_ptr<sdb::Lake_Stub> stub;//(&channel);
	brpc::Controller cntl;
	channel = std::make_unique<brpc::Channel>();

	LOG(ERROR) << "prepare upload";
	// Initialize the channel, NULL means using default options. 
	brpc::ChannelOptions options;
	options.protocol = "h2:grpc";
	//options.connection_type = "pooled";
	options.timeout_ms = 10000/*milliseconds*/;
	options.max_retry = 5;
	if (channel->Init("127.0.0.1", 10001, &options) != 0) {
		LOG(ERROR) << "AllocateXid: Fail to initialize channel";
		return false;
	}
	stub = std::make_unique<sdb::Lake_Stub>(channel.get());

	sdb::AllocateXidRequest request;
	request.set_dbid(dbid);
	request.set_sessionid(sid);
	request.set_read(read);
	request.set_write(write);

	sdb::AllocateXidResponse response;
	//request.set_message("I'm a RPC to connect stream");
	stub->AllocateXid(&cntl, &request, &response, NULL);
	if (cntl.Failed()) {
		LOG(ERROR) << "Fail to AllocateXid, " << cntl.ErrorText();
		return false;
	}
	if (read_xid != nullptr) {
		*read_xid = response.read_xid();
	}
	if (write_xid != nullptr) {
		*write_xid = response.write_xid();
	}
	commit_xid = response.write_xid();
	return true;
}
