/*-------------------------------------------------------------------------
 *
 * modify_state.cpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2022, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/modify_state.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/access/parquet/writer_state.hpp"

#include <sys/time.h>

#include <functional>
#include <list>

extern "C" {
#include "executor/executor.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
}

#include "backend/sdb/common/pg_export.hpp"
#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include "lake_service.pb.h"
#include "auto_mem.hpp"

/**
 * @brief Create a parquet modify state object
 *
 * @param reader_cxt memory context for reader
 * @param dirname directory path
 * @param s3_client aws s3 client
 * @param tuple_desc tuple descriptor
 * @param use_threads use_thread option
 * @param use_mmap use_mmap option
 * @return ParquetS3FdwModifyState* parquet modify state object
 */

ParquetS3WriterState *create_parquet_modify_state(
    MemoryContext reader_cxt, const char *dirname, Aws::S3::S3Client *s3_client,
    TupleDesc tuple_desc, std::set<int> target_attrs, bool use_threads,
    bool use_mmap) {
  return new ParquetS3WriterState(reader_cxt, dirname, s3_client, tuple_desc,
                                  target_attrs, use_threads, use_mmap);
}

/**
 * @brief Construct a new Parquet S 3 Fdw Modify State:: Parquet S 3 Fdw Modify
 * State object
 *
 * @param reader_cxt memory context for reader
 * @param dirname directory path
 * @param s3_client aws s3 client
 * @param tuple_desc tuple descriptor
 * @param use_threads use_thread option
 * @param use_mmap use_mmap option
 */
ParquetS3WriterState::ParquetS3WriterState(MemoryContext reader_cxt,
                                           const char *dir,
                                           Aws::S3::S3Client *s3_client,
                                           TupleDesc tuple_desc,
                                           std::set<int> target_attrs,
                                           bool use_threads, bool use_mmap)
    : cxt(reader_cxt),
      dirname(dir),
      s3_client(s3_client),
      tuple_desc(tuple_desc),
      target_attrs(target_attrs),
      use_threads(use_threads),
      use_mmap(use_mmap),
      schemaless(schemaless) {}

/**
 * @brief Destroy the Parquet S 3 Fdw Modify State:: Parquet S3 Fdw Modify State
 * object
 */
ParquetS3WriterState::~ParquetS3WriterState() {
	MemoryContextDelete(cxt);
}

/**
 * @brief add a new parquet file
 *
 * @param filename new file path
 * @param slot tuple table slot data
 * @return ParquetWriter* reader to new file
 */
std::shared_ptr<ParquetWriter> ParquetS3WriterState::NewInserter(
    const char *filename, TupleTableSlot *slot) {
  //auto old_cxt = MemoryContextSwitchTo(ctx);
  auto auto_switch = AutoSwitch(cxt);
  auto reader = CreateParquetWriter(rel_id, filename, tuple_desc);
  reader->SetRel(rel_name, rel_id);
  // reader->Open(filename, s3_client);
  // reader->set_options(use_threads, use_mmap);

  /* create temporary file */
  // reader->create_column_mapping(this->tuple_desc, this->target_attrs);
  // reader->create_new_file_temp_cache();

  reader->PrepareUpload();
  return reader;
}

/**
 * @brief check aws s3 client is existed
 *
 * @return true if s3_client is existed
 */
bool ParquetS3WriterState::HasS3Client() {
  if (this->s3_client) {
    return true;
  }
  return false;
}

/**
 * @brief upload all cached data on readers list
 */
void ParquetS3WriterState::Upload() {
  auto auto_switch = AutoSwitch(cxt);
  for (auto update : updates) {
    update.second->Upload(dirname.c_str(), s3_client);
    uploads_.push_back(update.second);
  }
  updates.clear();

  if (inserter_ != nullptr) {
    inserter_->Upload(dirname.c_str(), s3_client);
    uploads_.push_back(inserter_);
    inserter_ = nullptr;
  }

  //CommitUpload();
  for (auto upload : uploads_) {
    upload->CommitUpload();
  }
  uploads_.clear();
}

void ParquetS3WriterState::CommitUpload() {
	auto auto_switch = AutoSwitch(cxt);
	std::list<sdb::LakeFile> add_files;
	std::list<sdb::LakeFileHandle> delete_files;

	std::unique_ptr<brpc::Channel> channel;
	std::unique_ptr<sdb::Lake_Stub> stub;//(&channel);
	brpc::Controller cntl;
	channel = std::make_unique<brpc::Channel>();

	// Initialize the channel, NULL means using default options. 
	brpc::ChannelOptions options;
	options.protocol = "h2:grpc";
	options.connection_type = "pooled";
	options.timeout_ms = 10000/*milliseconds*/;
	options.max_retry = 5;
	if (channel->Init("127.0.0.1:10001", NULL) != 0) {
		LOG(ERROR) << "Fail to initialize channel";
		return;
	}
	stub = std::make_unique<sdb::Lake_Stub>(channel.get());


	sdb::UpdateFilesRequest request;
	for (auto it = add_files.begin(); it != add_files.end(); ++it) {
		auto f = request.add_add_files();
		*f = *it;
	}

	for (auto it = delete_files.begin(); it != delete_files.end(); ++it) {
		auto f = request.add_remove_files();
		*f = *it;
	}
	//auto add_file  = prepare_request->add_add_files();
	//add_file->set_file_name(file_name_);
	//add_file->set_space();
	sdb::UpdateFilesResponse response;
	//request.set_message("I'm a RPC to connect stream");
	stub->UpdateFiles(&cntl, &request, &response, NULL);
	if (cntl.Failed()) {
		LOG(ERROR) << "Fail to connect stream, " << cntl.ErrorText();
		return;
	}
}

/**
 * @brief insert a postgres tuple table slot to list parquet file
 *
 * @param slot tuple table slot
 * @return true if insert successfully
 */
bool ParquetS3WriterState::ExecInsert(TupleTableSlot *slot) {
  auto auto_switch = AutoSwitch(cxt);
  if (inserter_ != nullptr && inserter_->DataSize() > 100 * 1024 * 0124) {
	LOG(ERROR) << "upload " << inserter_->DataSize();
    inserter_->Upload(dirname.c_str(), s3_client);
    uploads_.push_back(inserter_);
    inserter_ = nullptr;
  }

  if (inserter_ == nullptr) {
    char uuid[1024];
    static uint32_t local_index = 0;
	const std::chrono::time_point<std::chrono::system_clock> now =
											std::chrono::system_clock::now();
	
    uint64_t worker_uuid =
	   std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    sprintf(uuid, "%s_%d_%lu_%u.parquet", rel_name, rel_id, worker_uuid, local_index++);
    inserter_ = NewInserter(uuid, slot);
  }
  if (inserter_ != nullptr) {
    return inserter_->ExecInsert(slot);
  }
  return false;
}

/**
 * @brief delete a record in list parquet file by key column
 *
 * @param slot tuple table slot
 * @param planSlot junk values
 * @return true if delete successfully
 */
bool ParquetS3WriterState::ExecDelete(ItemPointer tid) {
  auto auto_switch = AutoSwitch(cxt);
  uint64_t block_id = ItemPointerGetBlockNumber(tid);
  
  auto it = updates.find(block_id);
  if (it != updates.end()) {
    return it->second->ExecDelete(tid->ip_posid);
  }
  return false;
}

/**
 * @brief set relation name
 *
 * @param name relation name
 */
void ParquetS3WriterState::SetRel(char *name, Oid id) { 
	rel_name = name; 
	rel_id = id;
}

//oid ParquetS3WriterStateAddFile(uint64_t blockid, const char *filename)
