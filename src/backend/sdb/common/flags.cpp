#pragma once

#include <gflags/gflags.h>

DEFINE_int32(port, 40000, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_bool(gzip, false, "compress body using gzip");
DEFINE_int32(try_num, 20, "try numbers for brpc");
