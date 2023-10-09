#pragma once
#include <gflags/gflags.h>

DECLARE_string(dir);
DECLARE_string(database);
DECLARE_string(host);

DECLARE_uint64(dbid);
DECLARE_uint32(cluster);
DECLARE_int32(port);
DECLARE_int32(idle_timeout_s);
DECLARE_bool(gzip);
DECLARE_int32(try_num);
DECLARE_bool(reuse_port);
DECLARE_bool(reuse_addr);

DECLARE_string(endpoint);
DECLARE_string(s3user);
DECLARE_string(s3passwd);
DECLARE_string(region);
DECLARE_string(bucket);
DECLARE_bool(isminio);

DECLARE_string(schedule_host);
DECLARE_int32(schedule_port);

DECLARE_string(lake_host);
DECLARE_int32(lake_port);
