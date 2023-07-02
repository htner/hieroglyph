#ifndef PARQUETAM_H
#define PARQUETAM_H 

#include "access/relation.h"	/* for backward compatibility */
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/table.h"		/* for backward compatibility */
#include "access/tableam.h"
#include "nodes/lockoptions.h"
#include "nodes/primnodes.h"
#include "storage/bufpage.h"
#include "storage/lockdefs.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

extern HeapTuple ParquetGetNext(TableScanDesc sscan, ScanDirection direction);
extern TM_Result ParquetDelete(Relation relation, ItemPointer tid,
							CommandId cid, Snapshot crosscheck, bool wait,
							TM_FailureData *tmfd, bool changingPart);

extern void simple_parquet_insert(Relation relation, HeapTuple tup);
extern void simple_parquet_delete(Relation relation, ItemPointer tid);
extern void simple_parquet_update(Relation relation, ItemPointer otid,
							   HeapTuple tup);

extern void simple_parquet_insert_cache(Relation relation, HeapTuple tup);
extern void simple_parquet_upload(Relation relation);
extern void simple_parquet_uploadall();

#endif
