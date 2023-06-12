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
//#include "parquet_s3.h"

PG_MODULE_MAGIC;

void _PG_init(void);
extern void parquet_s3_init();
extern void parquet_s3_shutdown();

/* GUC variable */
bool parquet_use_threads;
bool enable_multifile;
bool enable_multifile_merge;

void _PG_init(void)
{
	DefineCustomBoolVariable("parquet_s3_fdw.use_threads",
							 "Enables use_thread option",
							 NULL,
							 &parquet_use_threads,
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

// PG_FUNCTION_INFO_V1(parquet_s3_fdw_validator);
// PG_FUNCTION_INFO_V1(parquet_s3_fdw_version);

PG_FUNCTION_INFO_V1(parquet_s3_fdw_handler);

/*
 * Appendonly access method uses virtual tuples
 */
static const TupleTableSlotOps *
ParquetSlotCallbacks(Relation relation)
{
	return &TTSOpsVirtual;
}

extern TableScanDesc
ParquetBeginScan(Relation relation,
				 Snapshot snapshot,
				 int nkeys, struct ScanKeyData *key,
				 ParallelTableScanDesc pscan,
				 uint32 flags);

extern TableScanDesc
ParquetBeginScanExtractColumns(Relation rel,
							   Snapshot snapshot,
							   List *targetlist,
							   List *qual,
							   bool *proj,
							   List *constraintList,
							   uint32 flags);

/*
 * GPDB: Extract columns for scan from targetlist and quals,
 * stored in key as struct ScanKeyData. This is mainly
 * for AOCS tables.
 */
extern TableScanDesc
ParquetBeginScanExtractColumnsBM(Relation rel, Snapshot snapshot,
								 List *targetList, List *quals,
								 List *bitmapqualorig,
								 uint32 flags);

extern void
ParquetEndScan(TableScanDesc scan);

extern void
ParquetRescan(TableScanDesc scan, ScanKey key,
			  bool set_params, bool allow_strat,
			  bool allow_sync, bool allow_pagemode);

extern bool
ParquetGetNextSlot(TableScanDesc scan,
				   ScanDirection direction,
				   TupleTableSlot *slot);

static Size
ParquetParallelScanEstimate(Relation rel)
{
	elog(ERROR, "parallel SeqScan not implemented for Parquet tables");
}

static Size
ParquetParallelScanInitialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "parallel SeqScan not implemented for Parquet tables");
}

static void
ParquetparallelScanReinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "parallel SeqScan not implemented for Parquet tables");
}

static IndexFetchTableData *
ParquetIndexFetchBegin(Relation rel)
{
	elog(ERROR, "not implemented for Parquet tables");
	return NULL;
}

static void
ParquetIndexFetchReset(IndexFetchTableData *scan)
{
	return;
}

static void
ParquetIndexFetchEnd(IndexFetchTableData *scan)
{
}

static bool
ParquetIndexFetchTuple(struct IndexFetchTableData *scan,
					   ItemPointer tid,
					   Snapshot snapshot,
					   TupleTableSlot *slot,
					   bool *call_again, bool *all_dead)
{
	return false;
}

static bool
ParquetIndexFetchTupleVisible(struct IndexFetchTableData *scan,
							  ItemPointer tid,
							  Snapshot snapshot)
{
	return true;
}

static bool
ParquetIndexUniqueCheck(Relation rel,
						ItemPointer tid,
						Snapshot snapshot,
						bool *all_dead)
{
	return true;
}

extern void
ParquetDmlInit(Relation rel);

extern void
ParquetDmlFinish(Relation rel);

extern void
ParquetTupleInsert(Relation relation, TupleTableSlot *slot, CommandId cid,
				   int options, BulkInsertState bistate);

extern void ParquetTupleInsertSpeculative(Relation relation, TupleTableSlot *slot,
										  CommandId cid, int options,
										  BulkInsertState bistate, uint32 specToken);

extern void
ParquetTupleCompleteSpeculative(Relation relation, TupleTableSlot *slot,
								uint32 specToken, bool succeeded);

extern void
ParquetMultiInsert(Relation relation, TupleTableSlot **slots, int ntuples,
				   CommandId cid, int options, BulkInsertState bistate);

extern TM_Result
ParquetTupleDelete(Relation relation, ItemPointer tid, CommandId cid,
				   Snapshot snapshot, Snapshot crosscheck, bool wait,
				   TM_FailureData *tmfd, bool changingPart);

extern TM_Result
ParquetTupleUpdate(Relation relation, ItemPointer otid, TupleTableSlot *slot,
				   CommandId cid, Snapshot snapshot, Snapshot crosscheck,
				   bool wait, TM_FailureData *tmfd,
				   LockTupleMode *lockmode, bool *update_indexes);

extern TM_Result
ParquetTupleLock(Relation relation, ItemPointer tid, Snapshot snapshot,
				 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				 LockWaitPolicy wait_policy, uint8 flags,
				 TM_FailureData *tmfd)
{
	return TM_Ok;
}

extern void
ParquetFinishBulkInsert(Relation relation, int options);

static bool
ParquetFetchRowVersion(Relation relation,
					   ItemPointer tid,
					   Snapshot snapshot,
					   TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not supported on appendoptimized relations")));
}

static void
ParquetGetLatestTid(TableScanDesc sscan,
					ItemPointer tid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not supported on appendoptimized relations")));
}

static bool
ParquetTupleTidValid(TableScanDesc scan, ItemPointer tid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not supported on appendoptimized relations")));
}

static bool
ParquetTupleSatisfiesSnapshot(Relation rel, TupleTableSlot *slot,
							  Snapshot snapshot)
{
	/*
	 * AO table dose not support unique and tidscan yet.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not supported on appendoptimized relations")));
}

static TransactionId
ParquetComputeXidHorizonForTuples(Relation rel,
								  ItemPointerData *tids,
								  int nitems)
{
	/*
	 * This API is only useful for hot standby snapshot conflict resolution
	 * (for eg. see btree_xlog_delete()), in the context of index page-level
	 * vacuums (aka page-level cleanups). This operation is only done when
	 * IndexScanDesc->kill_prior_tuple is true, which is never for AO/CO tables
	 * (we always return all_dead = false in the index_fetch_tuple() callback
	 * as we don't support HOT)
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("feature not supported on appendoptimized relations")));
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for appendonly AM.
 * ------------------------------------------------------------------------
 */
static void
ParquetRelationSetNewFilenode(Relation rel,
							  const RelFileNode *newrnode,
							  char persistence,
							  TransactionId *freezeXid,
							  MultiXactId *minmulti)
{
}

static void
ParquetRelationNontransactionalTruncate(Relation rel)
{
}

static void
ParquetRelationCopyData(Relation rel, const RelFileNode *newrnode)
{
}

static void
ParquetRelationCopyForCluster(Relation OldHeap, Relation NewHeap,
							  Relation OldIndex, bool use_sort,
							  TransactionId OldestXmin,
							  TransactionId *xid_cutoff,
							  MultiXactId *multi_cutoff,
							  double *num_tuples,
							  double *tups_vacuumed,
							  double *tups_recently_dead)
{
}

static void
ParquetVacuumRel(Relation onerel, VacuumParams *params,
				 BufferAccessStrategy bstrategy)
{
}

static bool
ParquetScanAnalyzeNextBlock(TableScanDesc scan, BlockNumber blockno,
							BufferAccessStrategy bstrategy)
{
	return false;
}

static bool
ParquetScanAnalyzeNextTuple(TableScanDesc scan, TransactionId OldestXmin,
							double *liverows, double *deadrows,
							TupleTableSlot *slot)
{
	return false;
}

static double
ParquetIndexBuildRangeScan(Relation heapRelation,
						   Relation indexRelation,
						   IndexInfo *indexInfo,
						   bool allow_sync,
						   bool anyvisible,
						   bool progress,
						   BlockNumber start_blockno,
						   BlockNumber numblocks,
						   IndexBuildCallback callback,
						   void *callback_state,
						   TableScanDesc scan)
{
	return 0.0;
}

static void
ParquetIndexValidateScan(Relation heapRelation,
						 Relation indexRelation,
						 IndexInfo *indexInfo,
						 Snapshot snapshot,
						 ValidateIndexState *state)
{
}

/* FDW routines */
extern uint64_t
ParquetRelationSize(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid);

static BlockSequence *
ParquetRelationGetBlockSequences(Relation rel,
								 int *numSequences)
{
}

static void
ParquetRelationGetBlockSequence(Relation rel,
								BlockNumber blkNum,
								BlockSequence *sequence)
{
}

static bool
ParquetRelationNeedsToastTable(Relation rel)
{
	return false;
}

static void
ParquetEstimateRelSize(Relation rel, int32 *attr_widths,
					   BlockNumber *pages, double *tuples,
					   double *allvisfrac)
{
}

static bool
ParquetScanBitmapNextBlock(TableScanDesc scan,
						   TBMIterateResult *tbmres)
{
}

static bool
ParquetScanBitmapNextTuple(TableScanDesc scan,
						   TBMIterateResult *tbmres,
						   TupleTableSlot *slot)
{
}

static bool
ParquetScanSampleNextBlock(TableScanDesc scan, SampleScanState *scanstate)
{
}

static bool
ParquetScanSampleNextTuple(TableScanDesc scan, SampleScanState *scanstate,
						   TupleTableSlot *slot)
{
}
/*
 * Release resources and deallocate scan. If TableScanDesc.temp_snap,
 * TableScanDesc.rs_snapshot needs to be unregistered.
 */
extern void (*scan_end)(TableScanDesc scan);

static const TableAmRoutine parquet_row_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = ParquetSlotCallbacks,

	.scan_begin = ParquetBeginScan,
	.scan_begin_extractcolumns = ParquetBeginScanExtractColumns,
	.scan_begin_extractcolumns_bm = ParquetBeginScanExtractColumnsBM,
	.scan_end = ParquetEndScan,
	.scan_rescan = ParquetRescan,
	.scan_getnextslot = ParquetGetNextSlot,

	.parallelscan_estimate = ParquetParallelScanEstimate,
	.parallelscan_initialize = ParquetParallelScanInitialize,
	.parallelscan_reinitialize = ParquetparallelScanReinitialize,

	.index_fetch_begin = ParquetIndexFetchBegin,
	.index_fetch_reset = ParquetIndexFetchReset,
	.index_fetch_end = ParquetIndexFetchEnd,
	.index_fetch_tuple = ParquetIndexFetchTuple,
	.index_fetch_tuple_visible = ParquetIndexFetchTupleVisible,
	.index_unique_check = ParquetIndexUniqueCheck,

	.dml_init = ParquetDmlInit,
	.dml_finish = ParquetDmlFinish,

	.tuple_insert = ParquetTupleInsert,
	.tuple_insert_speculative = ParquetTupleInsertSpeculative,
	.tuple_complete_speculative = ParquetTupleCompleteSpeculative,
	.multi_insert = ParquetMultiInsert,
	.tuple_delete = ParquetTupleDelete,
	.tuple_update = ParquetTupleUpdate,
	.tuple_lock = ParquetTupleLock,
	.finish_bulk_insert = ParquetFinishBulkInsert,

	.tuple_fetch_row_version = ParquetFetchRowVersion,
	.tuple_get_latest_tid = ParquetGetLatestTid,
	.tuple_tid_valid = ParquetTupleTidValid,
	.tuple_satisfies_snapshot = ParquetTupleSatisfiesSnapshot,
	.compute_xid_horizon_for_tuples = ParquetComputeXidHorizonForTuples,

	.relation_set_new_filenode = ParquetRelationSetNewFilenode,
	.relation_nontransactional_truncate = ParquetRelationNontransactionalTruncate,
	.relation_copy_data = ParquetRelationCopyData,
	.relation_copy_for_cluster = ParquetRelationCopyForCluster,
	.relation_vacuum = ParquetVacuumRel,
	.scan_analyze_next_block = ParquetScanAnalyzeNextBlock,
	.scan_analyze_next_tuple = ParquetScanAnalyzeNextTuple,
	.index_build_range_scan = ParquetIndexBuildRangeScan,
	.index_validate_scan = ParquetIndexValidateScan,

	.relation_size = ParquetRelationSize,
	.relation_get_block_sequences = ParquetRelationGetBlockSequences,
	.relation_get_block_sequence = ParquetRelationGetBlockSequence,
	.relation_needs_toast_table = ParquetRelationNeedsToastTable,

	.relation_estimate_size = ParquetEstimateRelSize,

	.scan_bitmap_next_block = ParquetScanBitmapNextBlock,
	.scan_bitmap_next_tuple = ParquetScanBitmapNextTuple,
	.scan_sample_next_block = ParquetScanSampleNextBlock,
	.scan_sample_next_tuple = ParquetScanSampleNextTuple};

Datum parquet_row_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&parquet_row_methods);
}
}
