#ifndef PG_EXPORT_HPP_sfawefew
#define PG_EXPORT_HPP_sfawefew
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

#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "commands/variable.h"
#include "utils/syscache.h"
#include "utils/rel.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/numeric.h"
#include "utils/fmgrprotos.h"

#include "parser/parser.h"
#include "optimizer/orca.h"
#include "sdb/postgres_init.h"

extern const char *error_severity(int elevel);
/*
{
	const char *prefix;

	switch (elevel)
	{
		case DEBUG1:
		case DEBUG2:
		case DEBUG3:
		case DEBUG4:
		case DEBUG5:
			prefix = gettext_noop("DEBUG");
			break;
		case LOG:
		case LOG_SERVER_ONLY:
			prefix = gettext_noop("LOG");
			break;
		case INFO:
			prefix = gettext_noop("INFO");
			break;
		case NOTICE:
			prefix = gettext_noop("NOTICE");
			break;
		case WARNING:
			prefix = gettext_noop("WARNING");
			break;
		case ERROR:
			prefix = gettext_noop("ERROR");
			break;
		case FATAL:
			prefix = gettext_noop("FATAL");
			break;
		case PANIC:
			prefix = gettext_noop("PANIC");
			break;
		default:
			prefix = "???";
			break;
	}

	return prefix;
}
*/


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

#define PG_LOG	15
#define PG_INFO 17
#define ELOG_INFO 17
#define ELOG_ERROR 20 

Bitmapset *
getExecParamsToDispatch(PlannedStmt *stmt, ParamExecData *intPrm,
						List **paramExecTypes);


SerializedParams *serializeParamsForDispatch(ParamListInfo externParams,
                                                    ParamExecData *execParams,
                                                    List *paramExecTypes,
                                                    Bitmapset *sendParams);
/*
void set_worker_param(int64_t sessionid, int64_t identifier);

void exec_worker_query(const char *query_string,
                       PlannedStmt	   *plan,
                       SerializedParams *paramInfo,
                       SliceTable *sliceTable,
					   const char *result_dir,
					   const char *result_file,
                       void* task);
*/

extern int PostPortNumber;
extern CommandDest whereToSendOutput;

PlannedStmt *utility_optimizer(Query *query);
void prepare_catalog(Oid *oid_arr, int size);
}

#pragma GCC diagnostic pop

#endif
