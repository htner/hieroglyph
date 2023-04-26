#pragma once
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wregister"

extern "C" {

#include "pg_config.h"
#include "c.h"
#include "postgres.h"
#include "nodes/pg_list.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "parser/parser.h"
#include "tcop/tcopprot.h"
#include "optimizer/orca.h"

#undef DEBUG5
#undef DEBUG4
#undef DEBUG3
#undef DEBUG2
#undef DEBUG1
#undef LOG
#undef LOG_SERVER_ONLY
#undef COMMERROR
#undef INFO
#undef NOTICE
#undef WARNING
#undef ERROR
#undef FATAL
#undef PANIC
#undef elog
#undef elogif

}

#pragma GCC diagnostic pop

