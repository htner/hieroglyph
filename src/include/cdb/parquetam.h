/*-------------------------------------------------------------------------
 *
 * cdbparquetam.h
 *	  append-only relation access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2007, Greenplum Inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbparquetam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBPARQUETAM_H
#define CDBPARQUETAM_H

#include "access/htup.h"
#include "access/memtup.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/tupmacs.h"
#include "access/xlogutils.h"
#include "access/xlog.h"
#include "access/parquet_visimap.h"
#include "executor/tuptable.h"
#include "nodes/primnodes.h"
#include "nodes/bitmapset.h"
#include "storage/block.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

#include "access/parquettid.h"

#include "cdb/cdbbufferedappend.h"
#include "cdb/cdbbufferedread.h"
#include "cdb/cdbvarblock.h"

#include "cdb/cdbparquetstoragelayer.h"
#include "cdb/cdbparquetstorageread.h"
#include "cdb/cdbparquetstoragewrite.h"
#include "cdb/cdbparquetblockdirectory.h"

#define DEFAULT_COMPRESS_LEVEL				 (0)
#define MIN_PARQUET_BLOCK_SIZE			 (8 * 1024)
#define DEFAULT_PARQUET_BLOCK_SIZE		(32 * 1024)
#define MAX_PARQUET_BLOCK_SIZE			 (2 * 1024 * 1024)
#define DEFAULT_VARBLOCK_TEMPSPACE_LEN   	 (4 * 1024)
#define DEFAULT_FS_SAFE_WRITE_SIZE			 (0)

/*
 * ParquetInsertDescData is used for inserting data into append-only
 * relations. It serves an equivalent purpose as ParquetScanDescData
 * (relscan.h) only that the later is used for scanning append-only 
 * relations. 
 */
typedef struct ParquetInsertDescData
{
	Relation		aoi_rel;
	MemTupleBinding *mt_bind;
	File			appendFile;
	int				appendFilePathNameMaxLen;
	char			*appendFilePathName;
	int64			insertCount;
	int64			varblockCount;
	int64           rowCount; /* total row count before insert */
	int64           numSequences; /* total number of available sequences */
	int64           lastSequence; /* last used sequence */
	BlockNumber		cur_segno;
	FileSegInfo     *fsInfo;
	VarBlockMaker	varBlockMaker;
	int64			bufferCount;
	int64			blockFirstRowNum;
	bool			usingChecksum;
	bool			useNoToast;
	bool			skipModCountIncrement;
	int32			completeHeaderLen;
	uint8			*tempSpace;

	int32			usableBlockSize;
	int32			maxDataLen;
	int32			tempSpaceLen;

	char						*title;
				/*
				 * A phrase that better describes the purpose of the this open.
				 *
				 * We manage the storage for this.
				 */

	/*
	 * These serve the equivalent purpose of the uppercase constants of the same
	 * name in tuptoaster.h but here we make these values dynamic.
	 */	
	int32			toast_tuple_threshold;
	int32			toast_tuple_target;
	ParquetStorageAttributes storageAttributes;
	ParquetStorageWrite		storageWrite;

	uint8			*nonCompressedData;

	/* The block directory for the parquet relation. */
	ParquetBlockDirectory blockDirectory;
	Oid segrelid;
} ParquetInsertDescData;

typedef ParquetInsertDescData *ParquetInsertDesc;

typedef struct ParquetExecutorReadBlock
{
	MemoryContext	memoryContext;

	ParquetStorageRead	*storageRead;

	MemTupleBinding *mt_bind;
	/*
	 * When reading a segfile that's using version < AOSegfileFormatVersion_GP5,
	 * that is, was created before GPDB 5.0 and upgraded with pg_upgrade, we need
	 * to convert numeric attributes on the fly to new format. numericAtts
	 * is an array of attribute numbers (0-based), of all numeric columns (including
	 * domains over numerics). This array is created lazily when first needed.
	 */
	int			   *numericAtts;
	int				numNumericAtts;

	int				segmentFileNum;

	int64			totalRowsScannned;

	int64			blockFirstRowNum;
	int64			headerOffsetInFile;
	uint8			*dataBuffer;
	int32			dataLen;
	int 			executorBlockKind;
	int 			rowCount;
	bool			isLarge;
	bool			isCompressed;

	uint8			*uncompressedBuffer; /* for decompression */

	uint8			*largeContentBuffer;
	int32			largeContentBufferLen;

	VarBlockReader  varBlockReader;
	int				readerItemCount;
	int				currentItemCount;
	
	uint8			*singleRow;
	int32			singleRowLen;
} ParquetExecutorReadBlock;

/*
 * Descriptor for append-only table scans.
 *
 * Used for scan of append only relations using BufferedRead and VarBlocks
 */
typedef struct ParquetScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */

	/* scan parameters */
	Relation	aos_rd;				/* target relation descriptor */
	Snapshot	appendOnlyMetaDataSnapshot;

	/*
	 * Snapshot to use for non-metadata operations.
	 * Usually snapshot = appendOnlyMetaDataSnapshot, but they
	 * differ e.g. if gp_select_invisible is set.
	 */ 
	Snapshot    snapshot;

	Index       aos_scanrelid;
	int			aos_nkeys;			/* number of scan keys */
	ScanKey		aos_key;			/* array of scan key descriptors */
	
	/* file segment scan state */
	int			aos_filenamepath_maxlen;
	char		*aos_filenamepath;
									/* the current segment file pathname. */
	int			aos_total_segfiles;	/* the relation file segment number */
	int			aos_segfiles_processed; /* num of segfiles already processed */
	FileSegInfo **aos_segfile_arr;	/* array of all segfiles information */
	bool		aos_need_new_segfile;
	bool		aos_done_all_segfiles;
	
	MemoryContext	aoScanInitContext; /* mem context at init time */

	int32			usableBlockSize;
	int32			maxDataLen;

	ParquetExecutorReadBlock	executorReadBlock;

	/* current scan state */
	bool		bufferDone;

	bool	initedStorageRoutines;

	ParquetStorageAttributes	storageAttributes;
	ParquetStorageRead		storageRead;

	char						*title;
				/*
				 * A phrase that better describes the purpose of the this open.
				 *
				 * We manage the storage for this.
				 */
	
	/*
	 * The block directory info.
	 *
	 * For AO tables, the block directory is built during the first index
	 * creation. If set indicates whether to build block directory while
	 * scanning.
	 */
	ParquetBlockDirectory *blockDirectory;

	/**
	 * The visibility map is used during scans
	 * to check tuple visibility using visi map.
	 */ 
	ParquetVisimap visibilityMap;

	/*
	 * Only used by `analyze`
	 */
	int64		nextTupleId;
	int64		targetTupleId;

	/* For Bitmap scan */
	int			rs_cindex;		/* current tuple's index in tbmres->offsets */
	struct ParquetFetchDescData *aofetch;

	/*
	 * The total number of bytes read, compressed, across all segment files, so
	 * far. This is used for scan progress reporting.
	 */
	int64		totalBytesRead;

}	ParquetScanDescData;

typedef ParquetScanDescData *ParquetScanDesc;

/*
 * Statistics on the latest fetch.
 */
typedef struct ParquetFetchDetail
{
	int64		rangeFileOffset;
	int64		rangeFirstRowNum;
	int64		rangeAfterFileOffset;
	int64		rangeLastRowNum;
					/*
					 * The range covered by the Block Directory.
					 */
	
	int64		skipBlockCount;
					/*
					 * Number of blocks skipped since the previous block processed in
					 * the range.
					 */
	
	int64		blockFileOffset;
	int32		blockOverallLen;
	int64		blockFirstRowNum;
	int64		blockLastRowNum;
	bool		isCompressed;
	bool		isLargeContent;
					/*
					 * The last block processed.
					 */

} ParquetFetchDetail;


/*
 * Used for fetch individual tuples from specified by TID of append only relations 
 * using the AO Block Directory, BufferedRead and VarBlocks
 */
typedef struct ParquetFetchDescData
{
	Relation		relation;
	Snapshot		appendOnlyMetaDataSnapshot;

	/*
	 * Snapshot to use for non-metadata operations.
	 * Usually snapshot = appendOnlyMetaDataSnapshot, but they
	 * differ e.g. if gp_select_invisible is set.
	 */ 
	Snapshot    snapshot;

	MemoryContext	initContext;

	ParquetStorageAttributes	storageAttributes;
	ParquetStorageRead		storageRead;

	char						*title;
				/*
				 * A phrase that better describes the purpose of the this open.
				 *
				 * We manage the storage for this.
				 */


	int				totalSegfiles;
	FileSegInfo 	**segmentFileInfo;

	char			*segmentFileName;
	int				segmentFileNameMaxLen;

	/*
	 * Array containing the maximum row number in each aoseg (to be consulted
	 * during fetch). This is a sparse array as not all segments are involved
	 * in a scan. Sparse entries are marked with InvalidAORowNum.
	 *
	 * Note:
	 * If we have no updates and deletes, the total_tupcount is equal to the
	 * maximum row number. But after some updates and deletes, the maximum row
	 * number is always much bigger than total_tupcount, so this carries the
	 * last sequence from gp_fastsequence.
	 */
	int64			lastSequence[AOTupleId_MultiplierSegmentFileNum];

	int32			usableBlockSize;

	ParquetBlockDirectory	blockDirectory;

	ParquetExecutorReadBlock executorReadBlock;

	AOFetchSegmentFile currentSegmentFile;
	
	int64		scanNextFileOffset;
	int64		scanNextRowNum;

	int64		scanAfterFileOffset;
	int64		scanLastRowNum;

	AOFetchBlockMetadata currentBlock;

	int64	skipBlockCount;

	ParquetVisimap visibilityMap;

}	ParquetFetchDescData;

typedef ParquetFetchDescData *ParquetFetchDesc;

/*
 * ParquetDeleteDescData is used for delete data from append-only
 * relations. It serves an equivalent purpose as ParquetScanDescData
 * (relscan.h) only that the later is used for scanning append-only
 * relations.
 */
typedef struct ParquetDeleteDescData
{
	/*
	 * Relation to delete from
	 */
	Relation	aod_rel;

	/*
	 * Snapshot to use for meta data operations
	 */
	Snapshot	appendOnlyMetaDataSnapshot;

	/*
	 * visibility map
	 */
	ParquetVisimap visibilityMap;

	/*
	 * Visimap delete support structure. Used to handle out-of-order deletes
	 */
	ParquetVisimapDelete visiMapDelete;

}			ParquetDeleteDescData;

typedef struct ParquetDeleteDescData *ParquetDeleteDesc;

typedef struct ParquetUniqueCheckDescData
{
	ParquetBlockDirectory *blockDirectory;
	ParquetVisimap 		 *visimap;
} ParquetUniqueCheckDescData;

typedef struct ParquetUniqueCheckDescData *ParquetUniqueCheckDesc;

typedef struct ParquetIndexOnlyDescData
{
	ParquetBlockDirectory *blockDirectory;
	ParquetVisimap 		 *visimap;
} ParquetIndexOnlyDescData, *ParquetIndexOnlyDesc;

/*
 * Descriptor for fetches from table via an index.
 */
typedef struct IndexFetchParquetData
{
	IndexFetchTableData xs_base;			/* AM independent part of the descriptor */

	ParquetFetchDesc aofetch;			/* used only for index scans */

	ParquetIndexOnlyDesc indexonlydesc;	/* used only for index only scans */
} IndexFetchParquetData;

/* ----------------
 *		function prototypes for parquet access method
 * ----------------
 */

extern ParquetScanDesc parquet_beginrangescan(Relation relation, 
		Snapshot snapshot,
		Snapshot appendOnlyMetaDataSnapshot, 
		int *segfile_no_arr, int segfile_count,
		int nkeys, ScanKey keys);

extern TableScanDesc parquet_beginscan(Relation relation,
										  Snapshot snapshot,
										  int nkeys, struct ScanKeyData *key,
										  ParallelTableScanDesc pscan,
										  uint32 flags);
extern void parquet_rescan(TableScanDesc scan, ScanKey key,
								bool set_params, bool allow_strat,
								bool allow_sync, bool allow_pagemode);
extern void parquet_endscan(TableScanDesc scan);
extern bool parquet_getnextslot(TableScanDesc scan,
								   ScanDirection direction,
								   TupleTableSlot *slot);
extern ParquetFetchDesc parquet_fetch_init(
	Relation 	relation,
	Snapshot    snapshot,
	Snapshot 	appendOnlyMetaDataSnapshot);
extern bool parquet_fetch(
	ParquetFetchDesc aoFetchDesc,
	AOTupleId *aoTid,
	TupleTableSlot *slot);
extern void parquet_fetch_finish(ParquetFetchDesc aoFetchDesc);
extern ParquetIndexOnlyDesc parquet_index_only_init(Relation relation,
														  Snapshot snapshot);
extern bool parquet_index_only_check(ParquetIndexOnlyDesc indexonlydesc,
										AOTupleId *aotid,
										Snapshot snapshot);
extern void parquet_index_only_finish(ParquetIndexOnlyDesc indexonlydesc);
extern void parquet_dml_init(Relation relation);
extern ParquetInsertDesc parquet_insert_init(Relation rel,
												   int segno,
												   int64 num_rows);
extern void parquet_insert(
		ParquetInsertDesc aoInsertDesc, 
		MemTuple instup, 
		AOTupleId *aoTupleId);
extern void parquet_insert_finish(ParquetInsertDesc aoInsertDesc);
extern void parquet_dml_finish(Relation relation);

extern ParquetDeleteDesc parquet_delete_init(Relation rel);
extern TM_Result parquet_delete(
		ParquetDeleteDesc aoDeleteDesc,
		AOTupleId* aoTupleId);
extern void parquet_delete_finish(ParquetDeleteDesc aoDeleteDesc);

/*
 * Update total bytes read for the entire scan. If the block was compressed,
 * update it with the compressed length. If the block was not compressed, update
 * it with the uncompressed length.
 */
static inline void
ParquetScanDesc_UpdateTotalBytesRead(ParquetScanDesc scan)
{
	Assert(scan->storageRead.isActive);

	if (scan->storageRead.current.isCompressed)
		scan->totalBytesRead += scan->storageRead.current.compressedLen;
	else
		scan->totalBytesRead += scan->storageRead.current.uncompressedLen;
}

#endif   /* CDBPARQUETAM_H */