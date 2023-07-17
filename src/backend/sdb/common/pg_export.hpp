#pragma once
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wregister"

extern "C" {

#include "pg_config.h"

#include "c.h"

#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/print.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "tcop/tcopprot.h"
#include "access/xact.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsrlz.h"
#include "executor/execdesc.h"

#include "parser/parser.h"
#include "optimizer/orca.h"

// TODO FIXME
// sp c / c++
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
#undef DAY
#undef SECOND
#undef IsPowerOf2
#undef Abs
#undef NIL

#define PG_LOG			15
#define PG_INFO 17
#define ELOG_INFO 17
#define ELOG_ERROR 20 

extern Bitmapset *
getExecParamsToDispatch(PlannedStmt *stmt, ParamExecData *intPrm,
						List **paramExecTypes);


extern SerializedParams *serializeParamsForDispatch(ParamListInfo externParams,
                                                    ParamExecData *execParams,
                                                    List *paramExecTypes,
                                                    Bitmapset *sendParams);

void set_worker_param(int64_t sessionid, int64_t identifier);

void exec_worker_query(const char *query_string,
                       PlannedStmt	   *plan,
                       SerializedParams *paramInfo,
                       SliceTable *sliceTable,
					   const char *result_dir,
					   const char *result_file,
                       void* task);

extern int PostPortNumber;

PlannedStmt *utility_optimizer(Query *query);
void prepare_catalog(List *prepare_catlog_list);

}

#pragma GCC diagnostic pop



