/*-------------------------------------------------------------------------
 *
 * wk_pgheader_wrap.h
 *    The pg header file required by wukongdb, undef some log macros, and
 *    prevent errors in the compilation of the glog.
 *
 * src/include/wukongdb/wk_pgheader_wrap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef WK_PGHEADER_WRAP_H
#define WK_PGHEADER_WRAP_H

#ifdef __cplusplus
extern "C" {
#endif
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wregister"

#include "postgres.h"
#include "executor/execdesc.h"
#include "nodes/plannodes.h"
#include "nodes/pg_list.h"
#include "cdb/cdbgang.h"

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

#pragma GCC diagnostic pop
#ifdef __cplusplus
};
#endif

#endif