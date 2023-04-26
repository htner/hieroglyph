/*-------------------------------------------------------------------------
 *
 * planner.h
 *    Wukongdb optimizer
 *
 * src/include/wukongdb/planner.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLANNER_H
#define PLANNER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "nodes/pg_list.h"

extern void wkdb_optimazer(List *plantree_list);

#ifdef __cplusplus
};
#endif

#endif