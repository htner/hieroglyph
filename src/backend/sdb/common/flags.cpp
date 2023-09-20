#include <gflags/gflags.h>

DEFINE_string(database, "template1", "database");
DEFINE_string(dir, "", "the base dir of postgres");
DEFINE_string(host, "127.0.0.1", "host");
DEFINE_uint64(dbid, 1, "database id");
DEFINE_uint32(cluster, 1, "cluster id");

DEFINE_int32(port, 40000, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_bool(gzip, false, "compress body using gzip");

DEFINE_int32(try_num, 20, "try numbers for brpc");
DEFINE_bool(optimizer, false, "is optimizer");
DEFINE_bool(worker, false, "is worker");

DEFINE_string(endpoint, "127.0.0.1:9000", "");
DEFINE_string(s3user, "minioadmin", "");
DEFINE_string(s3passwd, "minioadmin", "");
DEFINE_string(region, "us-east-1", "");
DEFINE_string(bucket, "sdb1", "");
DEFINE_bool(isminio, true, "");

//DEFINE_string(bucket, "sdb1", "");
