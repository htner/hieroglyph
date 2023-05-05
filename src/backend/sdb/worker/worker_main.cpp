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

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <brpc/restful.h>

#include "include/sdb/worker_main.h"
#include "include/sdb/postgres_init.h"

#include "backend/sdb/worker/execute_task.hpp"
#include "backend/sdb/worker/worker_service.hpp"

DECLARE_int32(port);
DECLARE_int32(idle_timeout_s);
DECLARE_bool(gzip);

int WorkerServerRun(int argc, char** argv);

int WorkerServiceMain(int argc, char* argv[]) {
	InitMinimizePostgresEnv(argc, argv, "sdb", "sdb");
	Gp_role = GP_ROLE_EXECUTE;
	std::thread worker_thread(WorkerServerRun, argc, argv);

	while (true) {
		auto task = sdb::ExecuteTaskQueueSingleton::GetInstance()->pop_front(); 
		LOG(ERROR) << "get one task";
		task->Run();
	}
	worker_thread.join();	
}

int WorkerServerRun(int argc, char** argv);
    // Parse gflags. We recommend you to use gflags as well.
    gflags::ParseCommandLineFlags(&argc, &argv, true);

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
