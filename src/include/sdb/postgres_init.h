#ifndef __POSTGETS_INIT_H
#define __POSTGETS_INIT_H

#ifdef __cplusplus
extern "C" {
#endif
void
InitMinimizePostgresEnv(const char* proc, const char* dir,
			 const char *dbname,
			 const char *username);

void 
set_worker_param(int64_t sessionid,
				 int64_t identifier);

void
exec_worker_query(const char *query_string,
				  PlannedStmt	   *plan,
				  SerializedParams *paramInfo,
				  SliceTable *sliceTable,
				  const char *result_dir,
				  const char *result_file,
				  void* task);

void prepare_catalog(Oid *oid_arr, int size);

PlannedStmt *utility_optimizer(Query *query);
#ifdef __cplusplus
}
#endif

#endif
