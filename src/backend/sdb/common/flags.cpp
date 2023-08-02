#include <gflags/gflags.h>

DEFINE_string(database, "template1", "database");
DEFINE_string(dir, "", "the base dir of postgres");
DEFINE_uint64(dbid, 1, "database id");

DEFINE_int32(port, 40000, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_bool(gzip, false, "compress body using gzip");

DEFINE_int32(try_num, 20, "try numbers for brpc");
DEFINE_bool(optimizer, false, "is optimizer");
DEFINE_bool(worker, false, "is worker");
