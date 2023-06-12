/*-------------------------------------------------------------------------
 *
 * parquet_fdw.c
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/parquet_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "storage/ipc.h"
#include "optimizer/planmain.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "nodes/execnodes.h"
#include "parquet_s3_fdw.h"


PG_MODULE_MAGIC;

void		_PG_init(void);
extern void parquet_s3_init();
extern void parquet_s3_shutdown();

/* FDW routines */
extern void parquetGetForeignRelSize(PlannerInfo *root,
									 RelOptInfo *baserel,
									 Oid foreigntableid);
extern void parquetGetForeignPaths(PlannerInfo *root,
								   RelOptInfo *baserel,
								   Oid foreigntableid);
extern ForeignScan *parquetGetForeignPlan(PlannerInfo *root,
										  RelOptInfo *baserel,
										  Oid foreigntableid,
										  ForeignPath *best_path,
										  List *tlist,
										  List *scan_clauses,
										  Plan *outer_plan);
extern TupleTableSlot *parquetIterateForeignScan(ForeignScanState *node);
extern void parquetBeginForeignScan(ForeignScanState *node, int eflags);
extern void parquetEndForeignScan(ForeignScanState *node);
extern void parquetReScanForeignScan(ForeignScanState *node);
extern void parquetAddForeignUpdateTargets(
#if (PG_VERSION_NUM >= 140000)
											 PlannerInfo *root,
											 Index rtindex,
#else
											 Query *parsetree,
#endif
											 RangeTblEntry *target_rte,
											 Relation target_relation);
extern List* parquetPlanForeignModify(PlannerInfo *root,
									  ModifyTable *plan,
									  Index resultRelation,
									  int subplan_index);
extern void parquetBeginForeignModify(ModifyTableState *mtstate,
									  ResultRelInfo *resultRelInfo,
									  List *fdw_private,
									  int subplan_index,
									  int eflags);
extern void parquetEndForeignModify(EState *estate,
									ResultRelInfo *resultRelInfo);
extern TupleTableSlot *parquetExecForeignUpdate(EState *estate,
												ResultRelInfo *resultRelInfo,
												TupleTableSlot *slot,
												TupleTableSlot *planSlot);
extern TupleTableSlot *parquetExecForeignInsert(EState *estate,
												ResultRelInfo *resultRelInfo,
												TupleTableSlot *slot,
												TupleTableSlot *planSlot);
extern TupleTableSlot *parquetExecForeignDelete(EState *estate,
												ResultRelInfo *resultRelInfo,
												TupleTableSlot *slot,
												TupleTableSlot *planSlot);
extern int	parquetAcquireSampleRowsFunc(Relation relation, int elevel,
										 HeapTuple *rows, int targrows,
										 double *totalrows,
										 double *totaldeadrows);
extern bool parquetAnalyzeForeignTable(Relation relation,
									   AcquireSampleRowsFunc *func,
									   BlockNumber *totalpages);
extern void parquetExplainForeignScan(ForeignScanState *node, ExplainState *es);
extern bool parquetIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
											 RangeTblEntry *rte);
extern Size parquetEstimateDSMForeignScan(ForeignScanState *node,
										  ParallelContext *pcxt);
extern void parquetInitializeDSMForeignScan(ForeignScanState *node,
											ParallelContext *pcxt,
											void *coordinate);
extern void parquetReInitializeDSMForeignScan(ForeignScanState *node,
											  ParallelContext *pcxt,
											  void *coordinate);
extern void parquetInitializeWorkerForeignScan(ForeignScanState *node,
											   shm_toc *toc,
											   void *coordinate);
extern void parquetShutdownForeignScan(ForeignScanState *node);
extern List *parquetImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);
extern Datum parquet_fdw_validator_impl(PG_FUNCTION_ARGS);

/* GUC variable */
extern bool parquet_fdw_use_threads;
extern bool enable_multifile;
extern bool enable_multifile_merge;

void
_PG_init(void)
{
	DefineCustomBoolVariable("parquet_s3_fdw.use_threads",
							 "Enables use_thread option",
							 NULL,
							 &parquet_fdw_use_threads,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	parquet_s3_init();

	on_proc_exit(&parquet_s3_shutdown, PointerGetDatum(NULL));
	DefineCustomBoolVariable("parquet_fdw.enable_multifile",
							"Enables Multifile reader",
							NULL,
							&enable_multifile,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("parquet_fdw.enable_multifile_merge",
							"Enables Multifile Merge reader",
							NULL,
							&enable_multifile_merge,
							true,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);
}

PG_FUNCTION_INFO_V1(parquet_s3_fdw_validator);
PG_FUNCTION_INFO_V1(parquet_s3_fdw_version);

Datum
parquet_s3_fdw_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(CODE_VERSION);
}

Datum
parquet_s3_fdw_validator(PG_FUNCTION_ARGS)
{
	return parquet_fdw_validator_impl(fcinfo);
}

PG_FUNCTION_INFO_V1(parquet_s3_fdw_handler);

Datum
parquet_s3_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = parquetGetForeignRelSize;
	fdwroutine->GetForeignPaths = parquetGetForeignPaths;
	fdwroutine->GetForeignPlan = parquetGetForeignPlan;
	fdwroutine->BeginForeignScan = parquetBeginForeignScan;
	fdwroutine->IterateForeignScan = parquetIterateForeignScan;
	fdwroutine->ReScanForeignScan = parquetReScanForeignScan;
	fdwroutine->EndForeignScan = parquetEndForeignScan;
	fdwroutine->AnalyzeForeignTable = parquetAnalyzeForeignTable;
	fdwroutine->ExplainForeignScan = parquetExplainForeignScan;
	fdwroutine->IsForeignScanParallelSafe = parquetIsForeignScanParallelSafe;
	fdwroutine->EstimateDSMForeignScan = parquetEstimateDSMForeignScan;
	fdwroutine->InitializeDSMForeignScan = parquetInitializeDSMForeignScan;
	fdwroutine->ReInitializeDSMForeignScan = parquetReInitializeDSMForeignScan;
	fdwroutine->InitializeWorkerForeignScan = parquetInitializeWorkerForeignScan;
	fdwroutine->ShutdownForeignScan = parquetShutdownForeignScan;
	fdwroutine->ImportForeignSchema = parquetImportForeignSchema;
	fdwroutine->AddForeignUpdateTargets = parquetAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = parquetPlanForeignModify;
	fdwroutine->BeginForeignModify = parquetBeginForeignModify;
	fdwroutine->ExecForeignUpdate = parquetExecForeignUpdate;
	fdwroutine->ExecForeignInsert = parquetExecForeignInsert;
	fdwroutine->ExecForeignDelete = parquetExecForeignDelete;
	fdwroutine->EndForeignModify = parquetEndForeignModify;

	PG_RETURN_POINTER(fdwroutine);
}

static const TableAmRoutine parquet_row_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = Parquetslot_callbacks,

	.scan_begin = ParquetBeginScan,
	.scan_begin_extractcolumns = ParquetBeginScanExtractColumns,
	.scan_begin_extractcolumns_bm = ParquetBeginScanExtractColumnsBM,
	.scan_end = ParquetEndScan,
	.scan_rescan = ParquetRescan,
	.scan_getnextslot = ParquetGetNextSlot,

	.parallelscan_estimate = Parquetparallelscan_estimate,
	.parallelscan_initialize = Parquetparallelscan_initialize,
	.parallelscan_reinitialize = Parquetparallelscan_reinitialize,

	.index_fetch_begin = Parquetindex_fetch_begin,
	.index_fetch_reset = Parquetindex_fetch_reset,
	.index_fetch_end = Parquetindex_fetch_end,
	.index_fetch_tuple = Parquetindex_fetch_tuple,
	.index_fetch_tuple_visible = Parquetindex_fetch_tuple_visible,
	.index_unique_check = Parquetindex_unique_check,

	.dml_init = Parquetdml_init,
	.dml_finish = Parquetdml_finish,

	.tuple_insert = Parquettuple_insert,
	.tuple_insert_speculative = Parquettuple_insert_speculative,
	.tuple_complete_speculative = Parquettuple_complete_speculative,
	.multi_insert = Parquetmulti_insert,
	.tuple_delete = Parquettuple_delete,
	.tuple_update = Parquettuple_update,
	.tuple_lock = Parquettuple_lock,
	.finish_bulk_insert = Parquetfinish_bulk_insert,

	.tuple_fetch_row_version = Parquetfetch_row_version,
	.tuple_get_latest_tid = Parquetget_latest_tid,
	.tuple_tid_valid = Parquettuple_tid_valid,
	.tuple_satisfies_snapshot = Parquettuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = Parquetcompute_xid_horizon_for_tuples,

	.relation_set_new_filenode = Parquetrelation_set_new_filenode,
	.relation_nontransactional_truncate = Parquetrelation_nontransactional_truncate,
	.relation_copy_data = Parquetrelation_copy_data,
	.relation_copy_for_cluster = Parquetrelation_copy_for_cluster,
	.relation_vacuum = Parquetvacuum_rel,
	.scan_analyze_next_block = Parquetscan_analyze_next_block,
	.scan_analyze_next_tuple = Parquetscan_analyze_next_tuple,
	.index_build_range_scan = Parquetindex_build_range_scan,
	.index_validate_scan = Parquetindex_validate_scan,

	.relation_size = Parquetrelation_size,
	.relation_get_block_sequences = Parquetrelation_get_block_sequences,
	.relation_get_block_sequence = Parquetrelation_get_block_sequence,
	.relation_needs_toast_table = Parquetrelation_needs_toast_table,

	.relation_estimate_size = Parquetestimate_rel_size,

	.scan_bitmap_next_block = Parquetscan_bitmap_next_block,
	.scan_bitmap_next_tuple = Parquetscan_bitmap_next_tuple,
	.scan_sample_next_block = Parquetscan_sample_next_block,
	.scan_sample_next_tuple = Parquetscan_sample_next_tuple
};

Datum
parquet_row_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&parquet_row_methods);
}
}
