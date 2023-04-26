/*-------------------------------------------------------------------------
 *
 * serialize_planstmt.h
 *    Provide serialized plan for executor.
 *
 * src/include/wukongdb/serialize_planstmt.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SERIALIZE_PLANSTMT_H
#define SERIALIZE_PLANSTMT_H

extern "C" {
#include "nodes/plannodes.h"
}

#include <string>

void serialize_pg_plan(PlannedStmt *plan_stmt, std::string &result);
#endif