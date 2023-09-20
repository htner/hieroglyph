// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// A server to receive HelloRequest and send back HelloReply

#include "backend/sdb/common/pg_export.hpp"

#include <thread>
#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <brpc/restful.h>
#include <bthread/unstable.h>
#include <gperftools/malloc_extension.h>

#include "include/sdb/worker_main.h"
#include "include/sdb/postgres_init.h"

#include "backend/sdb/worker/execute_task.hpp"
#include "backend/sdb/worker/worker_service.hpp"
#include "backend/sdb/common/common.hpp"
#include "backend/sdb/common/s3_context.hpp"
#include "backend/sdb/common/flags.hpp"

#include "schedule_service.pb.h"

extern "C" {
#include "include/sdb/session_info.h"
}

bool NewWorkerId();

void Ping() {
	std::unique_ptr<brpc::Channel> channel;
    std::unique_ptr<sdb::Schedule_Stub> stub;//(&channel);
    brpc::Controller cntl;
    channel = std::make_unique<brpc::Channel>();

    //LOG(ERROR) << "prepare upload";
    // Initialize the channel, NULL means using default options. 
    brpc::ChannelOptions options;
    options.protocol = "h2:grpc";
    //options.connection_type = "pooled";
    options.timeout_ms = 10000/*milliseconds*/;
    options.max_retry = 5;
    if (channel->Init("127.0.0.1", 10002, &options) != 0) {
      LOG(ERROR) << "PrepareUpload: Fail to initialize channel";
    } else {
		stub = std::make_unique<sdb::Schedule_Stub>(channel.get());

		auto& sess_info = thr_sess->session_cxt_;

		std::string addr = FLAGS_host + ":" + std::to_string(FLAGS_port);

		sdb::WorkerPingRequest request;
		request.set_worker_id(kWorkerId);
		request.set_cluster(FLAGS_cluster);
		request.set_state(kWorkerState);
		request.set_currect_index(kCurrentIndex);
		request.set_addr(addr);

		sdb::WorkerPongReply response;
		//request.set_message("I'm a RPC to connect stream");
		stub->WorkerPing(&cntl, &request, &response, NULL);
		if (cntl.Failed()) {
			LOG(ERROR) << "Fail to Ping, error" << cntl.ErrorText();
		}
	}
}

int WorkerServerRun(int argc, char** argv);

int WorkerServiceMain(int argc, char* argv[]) {
    // Parse gflags. We recommend you to use gflags as well.
    gflags::ParseCommandLineFlags(&argc, &argv, true);

	create_session_context(TopMemoryContext);
	FLAGS_reuse_addr = true;
	FLAGS_reuse_port = true;

	kNotInitdb = true;
	MyDatabaseId = FLAGS_dbid;
	MyDatabaseTableSpace = 1;

    auto& sess_info = thr_sess->session_cxt_;
	sess_info.dbid_ = FLAGS_dbid;
	sess_info.sessionid_ = 1;
	sess_info.query_id_ = 1;

	/*
	kDBBucket = FLAGS_bucket;
	kDBS3User = FLAGS_s3user;
	kDBS3Password = FLAGS_s3passwd;
	kDBS3Region = FLAGS_region;
	kDBS3Endpoint = FLAGS_endpoint;
	kDBIsMinio = FLAGS_isminio;
	*/

    auto s3_cxt = GetS3Context();
	s3_cxt->lake_bucket_ = FLAGS_bucket;
	s3_cxt->lake_user_ = FLAGS_s3user;
	s3_cxt->lake_password_ = FLAGS_s3passwd;
	s3_cxt->lake_region_ = FLAGS_region;
	s3_cxt->lake_endpoint_ = FLAGS_endpoint;
	s3_cxt->lake_isminio_ = FLAGS_isminio;

	sdb::CatalogInfo catalog_info;

	InitMinimizePostgresEnv(argv[0], FLAGS_dir.data(), FLAGS_database.data(), "root");

	Gp_role = GP_ROLE_EXECUTE;
	std::thread worker_thread(WorkerServerRun, argc, argv);

	NewWorkerId();


	while (true) {
		kCurrentIndex++;
		kWorkerState = sdb::WorkerState::WSReady;
		Ping();

		auto task = sdb::ExecuteTaskQueueSingleton::GetInstance()->pop_front(); 
		LOG(ERROR) << "get one task";

		kWorkerState = sdb::WorkerState::WSBusy;
		task->Run(catalog_info);
	}
	kWorkerState = sdb::WorkerState::WSStop;
	worker_thread.join();	
}

bool NewWorkerId() {
    std::unique_ptr<brpc::Channel> channel;
    std::unique_ptr<sdb::Schedule_Stub> stub;//(&channel);
    brpc::Controller cntl;
    channel = std::make_unique<brpc::Channel>();

    //LOG(ERROR) << "prepare upload";
    // Initialize the channel, NULL means using default options. 
    brpc::ChannelOptions options;
    options.protocol = "h2:grpc";
    //options.connection_type = "pooled";
    options.timeout_ms = 10000/*milliseconds*/;
    options.max_retry = 5;
    if (channel->Init("127.0.0.1", 10002, &options) != 0) {
      LOG(ERROR) << "PrepareUpload: Fail to initialize channel";
      return false;
    }
    stub = std::make_unique<sdb::Schedule_Stub>(channel.get());

    auto& sess_info = thr_sess->session_cxt_;

    sdb::NewWorkerIdRequest request;
    //request.set_worker_id(kWorkerId);
    request.set_cluster(FLAGS_cluster);
    //request.set_state(kWorkerState);
    //request.set_currect_index(kCurrentIndex);

    sdb::NewWorkerIdReply response;
    //request.set_message("I'm a RPC to connect stream");
    stub->NewWorkerId(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
      LOG(ERROR) << "Fail to New WorkerId " << cntl.ErrorText();
      return false;
    }
	kWorkerId = response.worker_id();
	kWorkerState = sdb::WorkerState::WSReady;
	return true;
}

// 定时任务的回调函数
static void timer_func(void* arg) {
	LOG(INFO) << "run timer fun:" << time(NULL);
    // 其他逻辑
    // ...
	// register_state
	Ping();

	bthread_timer_t timer;
    if (bthread_timer_add(&timer, butil::seconds_from_now(60),
                          timer_func, NULL) != 0) {
		LOG(ERROR) << "add timer task failed";
    }
	MallocExtension::instance()->ReleaseFreeMemory();
}

int WorkerServerRun(int argc, char** argv) {
	 // 1分钟后执行一次
    bthread_timer_t timer;
    if (bthread_timer_add(&timer, butil::seconds_from_now(60),
                          timer_func, NULL) != 0) {
		LOG(ERROR) << "add timer task failed";
    }

    // Generally you only need one Server.
    brpc::Server server;

	sdb::WorkerService http_svc;
    // Add services into server. Notice the second parameter, because the
    // service is put on stack, we don't want server to delete it, otherwise
    // use brpc::SERVER_OWNS_SERVICE.
    if (server.AddService(&http_svc,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add http_svc";
        return -1;
    }

	//FLAGS_port = PostPortNumber;
    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start HttpServer";
        return -1;
    }
    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}
