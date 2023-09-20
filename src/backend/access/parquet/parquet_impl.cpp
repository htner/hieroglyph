/*-------------------------------------------------------------------------
 *
 * parquet_impl.cpp
 *		  Parquet processing implementation for parquet_s3_fdw
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/parquet_impl.cpp
 *
 *-------------------------------------------------------------------------
 */
// basename comes from string.h on Linux,
// but from libgen.h on other POSIX systems (see man basename)
#ifndef GNU_SOURCE
//#include <libgen.h>
#endif

extern "C" {
#include "pg_config.h"
#include "c.h"
#include "postgres.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/parallel.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "catalog/index.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/appendinfo.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#else
#include "access/relation.h"
#include "access/table.h"
#include "access/valid.h"
#include "optimizer/optimizer.h"
#endif

#if PG_VERSION_NUM < 110000
#include "catalog/pg_am.h"
#else
#include "catalog/pg_am_d.h"
#endif
#include "include/sdb/session_info.h"
}

#include "backend/sdb/common/pg_export.hpp"

#include <math.h>
#include <sys/stat.h>

#include <list>
#include <set>

#include <arrow/api.h>
#include "arrow/array.h"
#include "arrow/io/api.h"
#include "backend/access/parquet/common.hpp"
#include "backend/access/parquet/reader_state.hpp"
#include "backend/access/parquet/heap.hpp"
#include "backend/access/parquet/writer_state.hpp"
#include "backend/access/parquet/parquet_reader.hpp"
#include "backend/access/parquet/parquet_s3/parquet_s3.hpp"
#include "backend/access/parquet/parquet_writer.hpp"
//#include "backend/access/parquet/slvars.hpp"
#include "backend/sdb/common/lake_file_mgr.hpp"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

#include "backend/sdb/common/singleton.hpp"
#include "backend/sdb/common/common.hpp"
#include "backend/sdb/common/s3_context.hpp"

/* from costsize.c */
#define LOG2(x) (log(x) / 0.693147180559945)

#if PG_VERSION_NUM < 110000
#define PG_GETARG_JSONB_P PG_GETARG_JSONB
#endif

#define IS_KEY_COLUMN(A) \
  ((strcmp(def->defname, "key") == 0) && (defGetBoolean(def) == true))

extern bool enable_multifile;
extern bool enable_multifile_merge;

extern void parquet_s3_init();

static void destroy_parquet_state(void *arg);
MemoryContext parquet_am_cxt = NULL;
/*
 * Restriction
 */
struct ParquetScanDescData {
  TableScanDescData rs_base;
  ParquetS3ReaderState *state;
};

typedef struct ParquetScanDescData *ParquetScanDesc;

struct RowGroupFilter {
  AttrNumber attnum;
  bool is_key; /* for maps */
  Const *value;
  int strategy;
  char *attname;  /* actual column name in schemales mode */
  Oid atttype;    /* Explicit cast type in schemaless mode
                      In non-schemaless NULL is expectation  */
  bool is_column; /* for schemaless actual column `exist` operator */
};

struct ExtractColumnsContext {
	std::vector<bool> * fetched_col;
	bool have_fetched;
};

using node_walker_type = bool (*)();

static std::unordered_map<Oid, ParquetS3WriterState *> fmstates;

ParquetS3WriterState *GetModifyState(Relation rel) {
  Oid oid = rel->rd_id;
  auto it = fmstates.find(oid);
  if (it != fmstates.end()) {
    return it->second;
  }
  return nullptr;
}

ParquetS3WriterState *CreateParquetModifyState(Relation rel,
                                               char *bucket,
                                               Aws::S3::S3Client *s3client,
                                               TupleDesc tuple_desc,
                                               bool use_threads) {
  Oid oid = rel->rd_id;
  auto fmstate = GetModifyState(rel);
  if (fmstate != NULL) {
    return fmstate;
  }

	if (parquet_am_cxt == nullptr) {
		parquet_am_cxt = AllocSetContextCreate(NULL, "parquet_s3_fdw temporary data",
										 ALLOCSET_DEFAULT_SIZES);
	}
  auto cxt = AllocSetContextCreate(parquet_am_cxt, "modify state temporary data",
                                   ALLOCSET_DEFAULT_SIZES);
  std::set<int> attrs;
  fmstate = create_parquet_modify_state(cxt, bucket, s3client, tuple_desc,
                                        attrs, use_threads, true);
  fmstates[oid] = fmstate;
  return fmstate;
}

typedef enum { PS_START = 0, PS_IDENT, PS_QUOTE } ParserState;

struct FieldInfo {
  char name[NAMEDATALEN];
  Oid oid;
};

static void destroy_parquet_state(void *arg) {
  ParquetS3ReaderState *festate = (ParquetS3ReaderState *)arg;
  if (festate) {
    delete festate;
  }
}

static void destroy_parquet_modify_state(void *arg) {
  ParquetS3WriterState *fmstate = (ParquetS3WriterState *)arg;

  if (fmstate && fmstate->HasS3Client()) {
    /*
     * After modify, parquet file information on S3 server is different with
     * cached one, so, disable connection imediately after modify to reload this
     * infomation.
     */
    //parquet_disconnect_s3_server();
    delete fmstate;
  }
}

/*
 * C interface functions
 */

static Aws::S3::S3Client *ParquetGetConnectionByRelation(Relation relation) {
  static bool init_s3sdk = false;
  if (!init_s3sdk) {
    parquet_s3_init();
    init_s3sdk = true;
  }
 
 auto s3_cxt = GetS3Context();
  Aws::S3::S3Client *s3client = s3_client_open(
	  s3_cxt->lake_user_.data(), s3_cxt->lake_password_.data(), s3_cxt->lake_isminio_, 
		s3_cxt->lake_endpoint_.data(), s3_cxt->lake_region_.data());
      //"minioadmin", "minioadmin", true, "127.0.0.1:9000", "ap-northeast-1");
  LOG(ERROR) << "parquet_s3 open:" <<
	  s3_cxt->lake_user_ << ", " << s3_cxt->lake_password_ << ", " <<
	  s3_cxt->lake_endpoint_ << ", " << s3_cxt->lake_region_;
  return s3client;
}

/*
static bool UsedColumnsWalker(Node *node, struct UsedColumnsContext *ctx) {
  if (node == NULL) {
    return false;
  }

  if (IsA(node, Var)) {
    Var *var = (Var *)node;

    if (IS_SPECIAL_VARNO(var->varno)) {
      return false;
    }

    if (var->varattno > 0 && var->varattno <= ctx->nattrs) {
      ctx->cols->insert(var->varattno - 1);
    } else if (var->varattno == 0) {
      for (AttrNumber attno = 0; attno < ctx->nattrs; attno++) {
        ctx->cols->insert(attno);
      }
      return true;
    }
    return false;
  }

  return expression_tree_walker(node, (bool (*)())UsedColumnsWalker,
                                (void *)ctx);
}

static bool GetUsedColumns(Node *node, AttrNumber nattrs,
                           std::set<int> *cols_out) {
  struct UsedColumnsContext ctx;
  ctx.cols = cols_out;
  ctx.nattrs = nattrs;
  return UsedColumnsWalker(node, &ctx);
}
*/

static bool ExtractColumnsWalker(Node *node, ExtractColumnsContext* cxt) {
	if (node == nullptr) {
		return false;
	}

	if (IsA(node, Var)) {
		Var *var = reinterpret_cast<Var *>(node);

		if (var->varattno > 0) {
			(*cxt->fetched_col)[var->varattno - 1] = true;
			cxt->have_fetched = true;
			return false;
		} else if (var->varattno == 0) {
			std::vector<bool>(cxt->fetched_col->size(), true).swap(*cxt->fetched_col);
			cxt->have_fetched = true;
			return true;
		}
	}

	return expression_tree_walker(node,
	 						(node_walker_type)ExtractColumnsWalker,
							(void *)cxt);
}

static void ExtractFetchedColumns(List *target_list, List *qual,
 								  std::vector<bool> &fetched_col) {
	if (target_list == nullptr && qual == nullptr) {
		std::vector<bool>(fetched_col.size(), true).swap(fetched_col);
		return;
	}

	ExtractColumnsContext cxt;
	cxt.fetched_col = &fetched_col;
	cxt.have_fetched = false;

    (void) ExtractColumnsWalker((Node *)target_list, &cxt);
	(void) ExtractColumnsWalker((Node *)qual, &cxt);
	/*
	 * In some cases (for example, count(*)), targetlist and qual may be null,
	 * ExtractColumnsWalker will return immediately, so no columns are specified.
	 * We always scan the first column.
	 */
	if (!cxt.have_fetched) {
		fetched_col[0] = true;
	}
}

static ParquetScanDesc ParquetBeginRangeScanInternal(
    Relation relation, Snapshot snapshot,
    // Snapshot appendOnlyMetaDataSnapshot,
    std::vector<sdb::LakeFile> lake_files, int nkeys, ScanKey key,
    ParallelTableScanDesc parallel_scan, List *targetlist, List *qual,
    List *bitmapqualorig, uint32 flags, struct DynamicBitmapContext *bmCxt) {
	ParquetS3ReaderState *state = NULL;
	ParquetScanDesc scan;

	std::vector<int> rowgroups;
	bool use_mmap = false;
	bool use_threads = false;
	Aws::S3::S3Client *s3client = NULL;
	ReaderType reader_type = RT_MULTI;
	int max_open_files = 10;

	// MemoryContextCallback *callback;
	MemoryContext reader_cxt;

	std::string error;

	RelationIncrementReferenceCount(relation);

	scan = (ParquetScanDesc)palloc0(sizeof(ParquetScanDescData));
	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	if (nkeys > 0) {
		scan->rs_base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
		memcpy(scan->rs_base.rs_key, key, nkeys * sizeof(ScanKeyData));
	} else {
		scan->rs_base.rs_key = NULL;
	}

	//GetUsedColumns((Node *)targetlist, tupDesc->natts, &attrs_used);

	s3client = ParquetGetConnectionByRelation(relation);

	TupleDesc tuple_desc = RelationGetDescr(relation);
	std::vector<bool> fetched_col(tuple_desc->natts, false);

	ExtractFetchedColumns(targetlist, qual, fetched_col);
	// TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	reader_cxt = AllocSetContextCreate(CurrentMemoryContext, "parquet_am tuple data",
									ALLOCSET_DEFAULT_SIZES);
	try {
		auto s3_cxt = GetS3Context();
		state = create_parquet_execution_state(
			reader_type, reader_cxt, s3_cxt->lake_bucket_.data(), s3client,
			relation->rd_id, tuple_desc, fetched_col,
			use_threads, use_mmap, max_open_files);

		for (size_t i = 0; i < lake_files.size(); ++i) {
			auto filename = std::to_string(lake_files[i].file_id()) + ".parquet";
			state->add_file(lake_files[i].file_id(), filename.c_str(), NULL);
		}
	} catch (std::exception &e) {
		error = e.what();
	}

	if (!error.empty()) {
		LOG(ERROR) << "parquet_am: " << error.c_str();
	}

	/*
   * Enable automatic execution state destruction by using memory context
   * callback
   */
	//callback = (MemoryContextCallback *)palloc(sizeof(MemoryContextCallback));
	//callback->func = destroy_parquet_state;
	//callback->arg = (void *)state;
	//MemoryContextRegisterResetCallback(reader_cxt, callback);

	scan->state = state;
	return scan;
}

extern "C" TableScanDesc ParquetBeginScan(Relation relation, Snapshot snapshot,
                                          int nkeys, struct ScanKeyData *key,
                                          ParallelTableScanDesc pscan,
                                          uint32 flags) {
  ParquetScanDesc parquet_desc;

  LOG(ERROR) << "parquet begin scan";
  /*
  seginfo = GetAllFileSegInfo(relation,
                                                          snapshot,
  &segfile_count, NULL);
                                                          */
  auto lake_files = ThreadSafeSingleton<sdb::LakeFileMgr>::GetInstance()->GetLakeFiles(relation->rd_id);
  for (size_t i = 0; i < lake_files.size(); ++i) {
		// LOG(ERROR) << lake_files[i].file_id(); // <<  " -> " << lake_files[i].file_name();
		//filenames.push_back(lake_files[i].file_name());
  }

  parquet_desc = ParquetBeginRangeScanInternal(relation, snapshot, lake_files,
                                               // appendOnlyMetaDataSnapshot,
                                               // seginfo,
                                               // segfile_count,
                                               nkeys, key, pscan, NULL, NULL,
                                               NULL, flags, NULL);

  return (TableScanDesc)parquet_desc;
}

extern "C"
TableScanDesc ParquetBeginScanExtractColumns(
    Relation rel, Snapshot snapshot, List *targetlist, List *qual, bool *proj,
    List *constraintList, uint32 flags) {
  ParquetScanDesc parquet_desc;

  LOG(INFO) << "parquet begin scan Extract Columns";
  /*
  seginfo = GetAllFileSegInfo(relation,
                                                          snapshot,
  &segfile_count, NULL);
                                                          */
  auto lake_files =
   	ThreadSafeSingleton<sdb::LakeFileMgr>::GetInstance()->GetLakeFiles(rel->rd_id);
  for (size_t i = 0; i < lake_files.size(); ++i) {
		LOG(INFO) << lake_files[i].file_id(); // <<  " -> " << lake_files[i].file_name();
		//filenames.push_back(lake_files[i].file_name());
  }

  parquet_desc = ParquetBeginRangeScanInternal(rel, snapshot, lake_files,
   									   0 /* nkeys */, nullptr /* ScanKey */,
   									   nullptr /* ParallelTableScanDesc*/,
									   targetlist, qual, nullptr /* bitmapqualorig*/,
									   flags, nullptr);

  return reinterpret_cast<TableScanDesc>(parquet_desc);
}

extern "C" HeapTuple ParquetGetNext(TableScanDesc sscan, ScanDirection direction) {
  ParquetScanDesc pscan = (ParquetScanDesc)sscan;
  ParquetS3ReaderState *festate = pscan->state;
  // TupleTableSlot             *slot = pscan->ss_ScanTupleSlot;
  std::string error;

  TupleTableSlot* slot = MakeSingleTupleTableSlot(RelationGetDescr(pscan->rs_base.rs_rd),
									&TTSOpsVirtual);
  try {
	while (true) {
			festate->next(slot);
			if (slot == nullptr) {
				LOG(ERROR) << "parquet get next slot nullptr";
				return nullptr;
			}

			bool		shouldFree = true;
			HeapTuple	tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

			ScanKey	  keys = pscan->rs_base.rs_key;
			int		  nkeys = pscan->rs_base.rs_nkeys;
			bool valid = true;
			bool result = true;
			if (keys != NULL) {
				//HeapKeyTest(tuple, RelationGetDescr(pscan->rs_base.rs_rd), nkeys, keys,
				//			valid);
				do 
				{ 
					/* Use underscores to protect the variables passed in as parameters */ 
					int			__cur_nkeys = (nkeys); 
					ScanKey		__cur_keys = (keys); 

					(result) = true; /* may change */ 
					for (; __cur_nkeys--; __cur_keys++) 
					{ 
						Datum	__atp; 
						bool	__isnull; 
						Datum	__test; 

						if (__cur_keys->sk_flags & SK_ISNULL) 
						{ 
							LOG(ERROR) << "SK_ISNULL:";
							(result) = false; 
							break; 
						} 

						__atp = heap_getattr((tuple), 
						   __cur_keys->sk_attno, 
						   (RelationGetDescr(pscan->rs_base.rs_rd)), 
						&__isnull); 

						if (__isnull) 
						{ 
							LOG(ERROR) << "ISNULL:";
							(result) = false; 
							break; 
						} 

						//LOG(ERROR) << "attr:" << DatumGetUInt32(__atp);
	 					__test = FunctionCall2Coll(&__cur_keys->sk_func, 
								 __cur_keys->sk_collation, 
								 __atp, __cur_keys->sk_argument); 

						if (!DatumGetBool(__test)) 
						{ 
							(result) = false; 
							break; 
						} 
					} 
				} while (0);
				LOG(ERROR) << "heap key test result" << valid;
			}
			valid = result;

		if (valid) {
				return tuple;
			}
	}
	LOG(ERROR) << "heap key get null ";
	
  } catch (std::exception &e) {
    error = e.what();
  }
  if (!error.empty()) {
    LOG(ERROR) << "ParquetGetNext:" << error.c_str();
    return nullptr;
  }

  return nullptr;
}

/*
 * sdb: it is uesd by heap_delete function
 */
extern "C" TM_Result ParquetDelete(Relation relation, ItemPointer tid,
							CommandId cid, Snapshot crosscheck, bool wait,
							TM_FailureData *tmfd, bool changingPart) {

	return TM_Ok;
}

extern "C" bool ParquetGetNextSlot(TableScanDesc scan, ScanDirection direction,
                                   TupleTableSlot *slot) {
	ParquetScanDesc pscan = (ParquetScanDesc)scan;
	ParquetS3ReaderState *festate = pscan->state;
	// TupleTableSlot             *slot = pscan->ss_ScanTupleSlot;
	std::string error;
	ExecClearTuple(slot);
	try {
		while(true) {
			// LOG(ERROR) << "parquet get next slot 1: " << error.c_str();
			bool ret = festate->next(slot);
			// LOG(ERROR) << "parquet get next slot: 2" << error.c_str();
			if (!ret) {
				LOG(ERROR) << "parquet get next slot return false";
				return false;
			}
			// LOG(ERROR) << "attr: 1 -> "<< DatumGetUInt32(slot->tts_values[0]);
			bool		shouldFree = true;
			HeapTuple	tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);

			//LOG(ERROR) << "parquet get next slot finish";
			ScanKey	  keys = pscan->rs_base.rs_key;
			int		  nkeys = pscan->rs_base.rs_nkeys;
			bool valid = true;
			bool result = true;
			if (keys != NULL) {
				// HeapKeyTest(tuple, RelationGetDescr(pscan->rs_base.rs_rd), nkeys, key, valid);
				// LOG(ERROR) << "heap key test result " << valid << " key size: " << nkeys << " attr no:" << keys[0].sk_attno;
				do { 
					/* Use underscores to protect the variables passed in as parameters */ 
					int			__cur_nkeys = (nkeys); 
					ScanKey		__cur_keys = (keys); 

					(result) = true; /* may change */ 
					for (; __cur_nkeys--; __cur_keys++) { 
						Datum	__atp; 
						bool	__isnull; 
						Datum	__test; 

						if (__cur_keys->sk_flags & SK_ISNULL) { 
							LOG(ERROR) << "SK_ISNULL:";
							(result) = false; 
							break; 
						} 

						__atp = heap_getattr((tuple), 
						   __cur_keys->sk_attno, 
						   (RelationGetDescr(pscan->rs_base.rs_rd)), 
						&__isnull); 

						if (__isnull) { 
							LOG(ERROR) << "ISNULL:";
							(result) = false; 
							break; 
						} 

						// LOG(ERROR) << "attr:" << __cur_keys->sk_attno  << " -> "<< DatumGetUInt32(__atp);
 	 					__test = FunctionCall2Coll(&__cur_keys->sk_func, 
								 __cur_keys->sk_collation, 
								 __atp, __cur_keys->sk_argument); 

						if (!DatumGetBool(__test)) { 
						//int *i = NULL;
						//*i = 0;
							(result) = false; 
							break; 
						} 
					} 
				} while (0);
				 //LOG(ERROR) << "heap key test result" << result;
			}
			valid = result;

			if (valid) {
				/*
				LOG(WARNING) << "get next tuple, fileid "
					<< ItemPointerGetBlockNumber(&(slot->tts_tid))
					<< " index " << ItemPointerGetOffsetNumber(&(slot->tts_tid))
					<< " tostring: " << ItemPointerToString(&(slot->tts_tid));
				*/
					return slot;
			}
		}
	} catch (std::exception &e) {
		error = e.what();
	}
	if (!error.empty()) {
		LOG(ERROR) << "parquet get next slot: " << error.c_str();
		std::string* e = nullptr;
		*e = error;
		return false;
	}
	LOG(WARNING) << "fetch next tuple, fileid "
	<< ItemPointerGetBlockNumber(&(slot->tts_tid))
	<< " index " << ItemPointerGetOffsetNumber(&(slot->tts_tid))
	<< " tostring: " << ItemPointerToString(&(slot->tts_tid));

	return true;
}

extern "C" void ParquetEndScan(TableScanDesc scan) {
  ParquetScanDesc pscan = (ParquetScanDesc)scan;
  ParquetS3ReaderState *festate = pscan->state;
  delete festate;
  pscan->state = nullptr;

  RelationDecrementReferenceCount(pscan->rs_base.rs_rd);
  if (pscan->rs_base.rs_key) {
	pfree(pscan->rs_base.rs_key);
  }
  if (pscan->rs_base.rs_flags & SO_TEMP_SNAPSHOT) {
	UnregisterSnapshot(pscan->rs_base.rs_snapshot);
  }

  pfree(pscan);
}

extern "C" void ParquetRescan(TableScanDesc scan, ScanKey key, bool set_params,
                              bool allow_strat, bool allow_sync,
                              bool allow_pagemode) {
  ParquetScanDesc pscan = (ParquetScanDesc)scan;
  ParquetS3ReaderState *festate = pscan->state;
  festate->rescan();
}

// single table test

extern "C" void ParquetDmlInit(Relation rel) {
  // Oid    foreignTableId = InvalidOid;
  TupleDesc tuple_desc;
  std::string error;
  arrow::Status status;
  std::set<std::string> sorted_cols;
  std::set<std::string> key_attrs;
  std::set<int> target_attrs;

  bool use_threads = true;
  // ListCell lc;

  // Oid tableId = RelationGetRelid(rel);
  tuple_desc = RelationGetDescr(rel);

  std::vector<std::string> filenames;

  // TODO
  for (int i = 0; i < tuple_desc->natts; ++i) {
    target_attrs.insert(i);
  }

	if (parquet_am_cxt == nullptr) {
		parquet_am_cxt = AllocSetContextCreate(NULL, "parquet_s3_fdw temporary data",
										 ALLOCSET_DEFAULT_SIZES);
	}

  auto s3client = ParquetGetConnectionByRelation(rel);
  try {
	auto s3_cxt = GetS3Context();
    auto fmstate = CreateParquetModifyState(rel, s3_cxt->lake_bucket_.data(), s3client,
                                            tuple_desc, use_threads);

    fmstate->SetRel(RelationGetRelationName(rel), RelationGetRelid(rel));
    LOG(WARNING)  << "set rel: " <<  RelationGetRelationName(rel) << " " <<  RelationGetRelid(rel);
	auto lake_files = ThreadSafeSingleton<sdb::LakeFileMgr>::GetInstance()->GetLakeFiles(rel->rd_id);
	for (size_t i = 0; i < lake_files.size(); ++i) {
		//LOG(INFO) << lake_files[i].file_name();
		LOG(ERROR) << lake_files[i].file_id(); //<<  " -> " << lake_files[i].file_name();
       //fmstate->add_file(i, filenames[i].data());
    }

    // if (plstate->selector_function_name)
    //     fmstate->set_user_defined_func(plstate->selector_function_name);
  } catch (std::exception &e) {
    error = e.what();
  }
  if (!error.empty()) {
    LOG(ERROR) << "parquet_s3_fdw: " << error.c_str();
  }

  /*
   * Enable automatic execution state destruction by using memory context
   * callback
   */
  // callback = (MemoryContextCallback *)palloc(sizeof(MemoryContextCallback));
  // callback->func = destroy_parquet_modify_state;
  // callback->arg = //(void *)fmstate;
  // MemoryContextRegisterResetCallback(estate->es_query_cxt, callback);
}

extern "C" void ParquetDmlFinish(Relation relation) {
	auto fmstate = GetModifyState(relation);
	if (fmstate == NULL) {
		return;
	}

	TupleTableSlot* slot = MakeSingleTupleTableSlot(RelationGetDescr(relation),
									&TTSOpsVirtual);

	std::unordered_set<uint64_t> lake_files_ids;
	auto lake_files = ThreadSafeSingleton<sdb::LakeFileMgr>::GetInstance()->GetLakeFiles(relation->rd_id);
	for (size_t i = 0; i < lake_files.size(); ++i) {
		lake_files_ids.insert(lake_files[i].file_id()); // <<  " -> " << lake_files[i].file_name();
		LOG(ERROR) << lake_files[i].file_id(); // <<  " -> " << lake_files[i].file_name();
		//filenames.push_back(lake_files[i].file_name());
	}
	// ParquetS3ReaderState *state;
	std::vector<int> rowgroups;
	bool use_mmap = false;
	bool use_threads = false;
	Aws::S3::S3Client *s3client = NULL;
	// ReaderType reader_type = RT_MULTI;
	// int max_open_files = 10;

	// MemoryContextCallback *callback;
	// MemoryContext reader_cxt;

	std::string error;

	//GetUsedColumns((Node *)targetlist, tupDesc->natts, &attrs_used);

	s3client = ParquetGetConnectionByRelation(relation);

	TupleDesc tuple_desc = RelationGetDescr(relation);
	std::vector<bool> fetched_col(tuple_desc->natts, true);

	// ExtractFetchedColumns(targetlist, qual, fetched_col);
	// TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	// reader_cxt = AllocSetContextCreate(NULL, "parquet_am tuple data",
	// ALLOCSET_DEFAULT_SIZES);
	try {
		/*
		state = create_parquet_execution_state(
			reader_type, reader_cxt, s3_cxt->lake_bucket_.data(), s3client,
			relation->rd_id, tuple_desc, fetched_col,
			use_threads, use_mmap, max_open_files);

		*/
		auto updates = fmstate->Updates();
		for (auto it = updates.begin(); it != updates.end(); ++it) {
			LOG(ERROR) << "update file " << it->second->FileId();
			if (lake_files_ids.find(it->second->FileId()) == lake_files_ids.end()) {
				LOG(ERROR) << "delete out of file";
				return;
			}
			auto s3_filename = std::to_string(it->second->FileId()) + ".parquet";
			auto deletes = it->second->Deletes();
			auto reader  = CreateParquetReader(relation->rd_id, it->second->FileId(),
									  s3_filename.data(), tuple_desc, fetched_col);
			reader->SetRowgroupsList(rowgroups);
			reader->SetOptions(use_threads, use_mmap);
			if (s3client) {
				auto s3_cxt = GetS3Context();
				auto status = reader->Open(s3_cxt->lake_bucket_.c_str(), s3client);
				if (!status.ok()) {
					LOG(ERROR) << "open failure";
					break;
				}
			}
			ReadStatus  res;
			while(true) {
				res = reader->Next(slot);
				if (res != RS_SUCCESS) {
					LOG(ERROR) << "parquet get next slot nullptr";
					break;
				}
				LOG(ERROR) << "get from old file, pos" << slot->tts_tid.ip_posid;
				if (deletes.find(slot->tts_tid.ip_posid) != deletes.end()) {
					continue;
				} else {
					LOG(ERROR) << "insert back to new file, pos" << slot->tts_tid.ip_posid;
					fmstate->ExecInsert(slot);
				}
			}
		}
	} catch (std::exception &e) {
		error = e.what();
	}
	fmstate->Upload();
}

/*
 * sdb: it is uesd by heap_insert function
 */
extern "C" void ParquetInsert(Relation rel, HeapTuple tuple, CommandId cid,
                              int options, struct BulkInsertStateData *bistate,
                              TransactionId xid) {
	if (parquet_am_cxt == NULL) {
		parquet_am_cxt = AllocSetContextCreate(NULL, "parquet_s3_fdw temporary data",
								  ALLOCSET_DEFAULT_SIZES);
	}
	auto old_cxt = MemoryContextSwitchTo(parquet_am_cxt);
	std::string error;
	TupleTableSlot *slot;
	TupleDesc desc;
	desc = RelationGetDescr(rel);
	LOG(INFO) << "parquet insert finish: " << error.c_str();
	//slot = MakeTupleTableSlot(desc, &TTSOpsVirtual);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
	&TTSOpsVirtual);
	slot->tts_tableOid = RelationGetRelid(rel);
	heap_deform_tuple(tuple, RelationGetDescr(rel), slot->tts_values, slot->tts_isnull);
	ExecStoreVirtualTuple(slot);
	//ExecStoreHeapTuple(tuple, slot, true);

	auto fmstate = GetModifyState(rel);

	if (fmstate == nullptr) {
		auto s3client = ParquetGetConnectionByRelation(rel);
		auto s3_cxt = GetS3Context();
		fmstate =
			CreateParquetModifyState(rel, s3_cxt->lake_bucket_.data(), s3client, desc, true);

		fmstate->SetRel(RelationGetRelationName(rel), RelationGetRelid(rel));
		LOG(WARNING) << "set rel: " << RelationGetRelationName(rel) << " " << RelationGetRelid(rel);
	}
	//ExecStoreVirtualTuple(slot);

	try {
		fmstate->ExecInsert(slot);
		fmstate->Upload();
		//LOG(INFO) << "parquet insert finish ok?";
		// if (plstate->selector_function_name)
		//     fmstate->set_user_defined_func(plstate->selector_function_name);
	} catch (std::exception &e) {
		error = e.what();
	}
	if (!error.empty()) {
		LOG(ERROR) << "parquet insert error: " << error.c_str();
	}

	// LOG(INFO) << "parquet insert finish: " << error.c_str();

	MemoryContextSwitchTo(old_cxt);
	// return slot;
}

extern "C" void ParquetTupleInsert(Relation rel, TupleTableSlot *slot,
                                   CommandId cid, int options,
                                   struct BulkInsertStateData *bistate) {
  auto fmstate = GetModifyState(rel);
  if (fmstate == NULL) {
    Assert(false);
  }
  fmstate->ExecInsert(slot);
  // return slot;
}

/*
 *      Update one row
 */
extern "C" TM_Result ParquetTupleUpdate(Relation rel, ItemPointer otid,
                                        TupleTableSlot *slot, CommandId cid,
                                        Snapshot snapshot, Snapshot crosscheck,
                                        bool wait, TM_FailureData *tmfd,
                                        LockTupleMode *lockmode,
                                        bool *update_indexes) {
  auto fmstate = GetModifyState(rel);
  if (fmstate == NULL) {
    return TM_Ok;
  }
  if (!fmstate->ExecDelete(otid)) {
	return TM_Deleted;
  }
  fmstate->ExecInsert(slot);
  return TM_Ok;
}

extern "C" TM_Result ParquetTupleDelete(Relation relation, ItemPointer tid,
                                        CommandId cid, Snapshot snapshot,
                                        Snapshot crosscheck, bool wait,
                                        TM_FailureData *tmfd,
                                        bool changingPart) {
  auto fmstate = GetModifyState(relation);
  if (fmstate == NULL) {
    return TM_Ok;
  }
  if (!fmstate->ExecDelete(tid)) {
	return TM_Deleted;
  }
  return TM_Ok;
}


extern "C" void ParquetWriterUpload() {
  auto it = fmstates.begin();
  for (; it != fmstates.end(); ++it) {
    it->second->Upload();
  }
  fmstates.clear();
}

extern "C"
void simple_parquet_insert_cache(Relation rel, HeapTuple tuple) {
	if (parquet_am_cxt == nullptr) {
		parquet_am_cxt = AllocSetContextCreate(NULL, "parquet_s3_fdw temporary data",
			ALLOCSET_DEFAULT_SIZES);
	}
	auto old_cxt = MemoryContextSwitchTo(parquet_am_cxt);
	std::string error;
	TupleTableSlot *slot;
	TupleDesc desc;
	desc = RelationGetDescr(rel);
	//LOG(INFO) << "parquet insert finish: " << error.c_str();
	//slot = MakeTupleTableSlot(desc, &TTSOpsVirtual);
	slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
	&TTSOpsVirtual);
	slot->tts_tableOid = RelationGetRelid(rel);
	heap_deform_tuple(tuple, RelationGetDescr(rel), slot->tts_values, slot->tts_isnull);
	ExecStoreVirtualTuple(slot);
	//ExecStoreHeapTuple(tuple, slot, true);

	auto fmstate = GetModifyState(rel);

	if (fmstate == nullptr) {
		auto s3client = ParquetGetConnectionByRelation(rel);
		auto s3_cxt = GetS3Context();
		fmstate =
			CreateParquetModifyState(rel, s3_cxt->lake_bucket_.data(), s3client, desc, true);

		fmstate->SetRel(RelationGetRelationName(rel), RelationGetRelid(rel));
		//LOG(WARNING) << "set rel: " << RelationGetRelationName(rel) << " " << RelationGetRelid(rel);
	}
	//ExecStoreVirtualTuple(slot);

	try {
		fmstate->ExecInsert(slot);
		// LOG(INFO) << "parquet insert finish ok?";
		// if (plstate->selector_function_name)
		//     fmstate->set_user_defined_func(plstate->selector_function_name);
	} catch (std::exception &e) {
		error = e.what();
	}
	if (!error.empty()) {
		LOG(ERROR) << "parquet_s3_fdw: " << error.c_str();
	}
	//LOG(INFO) << "parquet insert finish: " << error.c_str();
	MemoryContextSwitchTo(old_cxt);

}

extern "C"
void simple_parquet_upload(Relation rel) {
	std::string error;
	auto fmstate = GetModifyState(rel);
	if (fmstate == nullptr) {
		return;
	}
	try {
		fmstate->Upload();
		// LOG(INFO) << "parquet upload finish"; 
		// if (plstate->selector_function_name)
		//     fmstate->set_user_defined_func(plstate->selector_function_name);
	} catch (std::exception &e) {
		error = e.what();
	}
	if (!error.empty()) {
		LOG(ERROR) << "upload: " << error.c_str();
	}
	// 	LOG(INFO) << "parquet upload finish: " << error.c_str();
}

extern "C"
void simple_parquet_uploadall() {
  auto it = fmstates.begin();
  while (it != fmstates.end()) {
    it->second->Upload();
	destroy_parquet_modify_state(it->second);	
	it++;
  }
  fmstates.clear();
  
  LOG(ERROR) << "uploadall";
}

typedef struct IndexFetchParquetData
{
	IndexFetchTableData xs_base;			/* AM independent part of the descriptor */
    ParquetS3ReaderState *state;

	//AppendOnlyFetchDesc aofetch;			/* used only for index scans */

	//AppendOnlyIndexOnlyDesc indexonlydesc;	/* used only for index only scans */
} IndexFetchParquetData;


extern "C"
IndexFetchTableData *ParquetIndexFetchBegin(Relation relation) {
  //elog(ERROR, "not implemented for Parquet tables");
	IndexFetchParquetData *scan = (IndexFetchParquetData*)palloc0(sizeof(IndexFetchParquetData));
    ParquetS3ReaderState *state;
	bool use_mmap = false;
	bool use_threads = false;
	ReaderType reader_type = RT_MULTI;
	int max_open_files = 10;

	std::string error;
	scan->xs_base.rel = relation;

	/* aoscan->aofetch is initialized lazily on first fetch */
	auto s3client = ParquetGetConnectionByRelation(relation);

	TupleDesc tuple_desc = RelationGetDescr(relation);
	// TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	std::vector<bool> fetched_col(tuple_desc->natts, true);

	auto reader_cxt = AllocSetContextCreate(CurrentMemoryContext, "parquet_am tuple data",
									ALLOCSET_DEFAULT_SIZES);
	try {

		auto s3_cxt = GetS3Context();
		state = create_parquet_execution_state(
			reader_type, reader_cxt, s3_cxt->lake_bucket_.data(), s3client, relation->rd_id, tuple_desc,
			fetched_col, use_threads, use_mmap, max_open_files);

		auto lake_files = ThreadSafeSingleton<sdb::LakeFileMgr>::GetInstance()->GetLakeFiles(relation->rd_id);

		for (size_t i = 0; i < lake_files.size(); i++) {
			auto filename = std::to_string(lake_files[i].file_id()) + ".parquet";
			// FIXME_SDB add space id future
			state->add_file(lake_files[i].file_id(), filename.data(), NULL);
		}
	} catch (std::exception &e) {
		error = e.what();
	}

	if (!error.empty()) {
		LOG(ERROR) << "parquet_am: " << error.c_str();
	}

	scan->state = state;
	return &scan->xs_base;
}

extern "C"
void ParquetIndexFetchReset(IndexFetchTableData *scan) {
  LOG(ERROR) << "parallel SeqScan not implemented for Parquet tables";
  return;
}

extern "C"
void ParquetIndexFetchEnd(IndexFetchTableData *scan) {
  IndexFetchParquetData *iscan = (IndexFetchParquetData*)scan;
  //LOG(ERROR) << "parallel SeqScan not implemented for Parquet tables";
  delete iscan->state;
  iscan->state = nullptr;
  pfree(iscan);
}

extern "C"
bool ParquetIndexFetchTuple(struct IndexFetchTableData *scan,
                                   ItemPointer tid, Snapshot snapshot,
                                   TupleTableSlot *slot, bool *call_again,
                                   bool *all_dead) {
	IndexFetchParquetData *pscan = (IndexFetchParquetData*) scan;
	ParquetS3ReaderState *festate = pscan->state;
	// TupleTableSlot             *slot = pscan->ss_ScanTupleSlot;
	std::string error;
	//std::string *e = nullptr;
	//error = *e;


	LOG(ERROR) << "parquet index fetch tuple: " << ItemPointerToString(tid);
	try {
		bool ret = festate->fetch(tid, slot, true);
		if (!ret) {
			LOG(ERROR) << "parquet index fetch slot return false";
			return false;
		}
	} catch (std::exception &e) {
		error = e.what();
	}
	if (!error.empty()) {
		LOG(ERROR) << "parquet get next slot: " << error.c_str();
		return false;
	}
	if (all_dead)
		*all_dead = false;

	/* Currently, we don't determine this parameter. By contract, it is to be
	 * set to true iff there is another tuple for the tid, so that we can prompt
	 * the caller to call index_fetch_tuple() again for the same tid.
	 * This is typically used for HOT chains, which we don't support.
	 */
	if (call_again)
		*call_again = false;

	return !TupIsNull(slot);
}

extern "C"
bool ParquetIndexFetchTupleVisible(struct IndexFetchTableData *scan,
                                          ItemPointer tid, Snapshot snapshot) {
  LOG(ERROR) << "parallel SeqScan not implemented for Parquet tables";
  return true;
}

extern "C"
bool ParquetIndexUniqueCheck(Relation rel, ItemPointer tid,
                                    Snapshot snapshot, bool *all_dead) {
  LOG(ERROR) << "parallel SeqScan not implemented for Parquet tables";
  return true;
}

extern "C"
double ParquetIndexBuildRangeScan(
    Relation heapRelation, Relation indexRelation, IndexInfo *indexInfo,
    bool allow_sync, bool anyvisible, bool progress, BlockNumber start_blockno,
    BlockNumber numblocks, IndexBuildCallback callback, void *callback_state,
    TableScanDesc scan) {
	ParquetScanDesc parquet_scan;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	double		reltuples;
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	Snapshot	snapshot;

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/* Appendoptimized catalog tables are not supported. */
	/* Appendoptimized tables have no data on coordinator. */
	if (IS_QUERY_DISPATCHER())
		return 0;

	/* See whether we're verifying uniqueness/exclusion properties */
	//checking_uniqueness = (indexInfo->ii_Unique ||
	//						   indexInfo->ii_ExclusionOps != NULL);

	/*
	 * "Any visible" mode is not compatible with uniqueness checks; make sure
	 * only one of those is requested.
	 */
	Assert(!(anyvisible && checking_uniqueness));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(heapRelation, NULL);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	if (!scan)
	{
		/*
		 * Serial index build.
		 *
		 * XXX: We always use SnapshotAny here. An MVCC snapshot and oldest xmin
		 * calculation is necessary to support indexes built CONCURRENTLY.
		 */
		snapshot = SnapshotAny;
		scan = table_beginscan_strat(heapRelation,	/* relation */
									 snapshot,	/* snapshot */
									 0, /* number of keys */
									 NULL,	/* scan key */
									 true,	/* buffer access strategy OK */
									 allow_sync);	/* syncscan OK? */
	}
	else
	{
		/*
		 * Parallel index build.
		 *
		 * Parallel case never registers/unregisters own snapshot.  Snapshot
		 * is taken from parallel heap scan, and is SnapshotAny or an MVCC
		 * snapshot, based on same criteria as serial case.
		 */
		Assert(!IsBootstrapProcessingMode());
		Assert(allow_sync);
		snapshot = scan->rs_snapshot;
	}

	parquet_scan = (ParquetScanDesc) scan;


	/*
	 * Scan all tuples in the base relation.
	 */
	while (ParquetGetNextSlot(&parquet_scan->rs_base, ForwardScanDirection, slot)) {
		bool		tupleIsAlive;
		//AOTupleId 	*aoTupleId;

		CHECK_FOR_INTERRUPTS();

		/*
		 * GPDB_12_MERGE_FIXME: How to properly do a partial scan? Currently,
		 * we scan the whole table, and throw away tuples that are not in the
		 * range. That's clearly very inefficient.
		 */
		if (ItemPointerGetBlockNumber(&slot->tts_tid) < start_blockno ||
			(numblocks != InvalidBlockNumber && ItemPointerGetBlockNumber(&slot->tts_tid) >= numblocks))
			continue;

		tupleIsAlive = true;
		reltuples += 1;
		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * For the current heap tuple, extract all the attributes we use in
		 * this index, and note which are null.  This also performs evaluation
		 * of any expressions needed.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/*
		 * You'd think we should go ahead and build the index tuple here, but
		 * some index AMs want to do further processing on the data first.  So
		 * pass the values[] and isnull[] arrays, instead.
		 */

		/* Call the AM's callback routine to process the tuple */
		/*
		 * GPDB: the callback is modified to accept ItemPointer as argument
		 * instead of HeapTuple.  That allows the callback to be reused for
		 * appendoptimized tables.
		 */
		callback(indexRelation, &slot->tts_tid, values, isnull, tupleIsAlive,
				 callback_state);

	}

	table_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NULL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
}
