#ifndef __POSTGETS_INIT_H_SEFEWF
#define __POSTGETS_INIT_H_SEFEWF 1

void
InitMinimizePostgresEnv(const char* proc, const char* dir,
			 const char *dbname,
			 const char *username);

void 
set_worker_param(int64_t sessionid,
				 int64_t identifier);

bool
exec_worker_query(const char *query_string,
				  PlannedStmt	   *plan,
				  SerializedParams *paramInfo,
				  SliceTable *sliceTable,
				  const char *result_dir,
				  const char *result_file,
				  uint64_t* process_rows,
				  void* task);

void prepare_catalog(Oid *oid_arr, int size);

PlannedStmt *utility_optimizer(Query *query);

void FetchRelationOidFromQuery(Query *query, List **read_list, List **insert_list, List **update_lsit, List **delete_list);

PlannedStmt *utility_optimizer(Query *query);

void prepare_catalog(Oid *oid_arr, int size);

#endif
