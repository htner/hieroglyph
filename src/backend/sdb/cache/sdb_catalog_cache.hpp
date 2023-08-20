
_Pragma("once")

extern "C" {
#include "c.h"
#include "postgres.h"
#include "utils/catcache.h"
#include "access/tupdesc.h"
}

#include "sdb_cache_info.hpp"
#include "local_cache.hpp"
#include <vector>
#include <unordered_set>

namespace sdb {

class CatalogCache {
private:
using Cache = LocalCache<std::string, std::string, sdb::LRUPolicy<std::string>,
						sdb::NoUseDisk<std::string, std::string>>;

struct CatCacheData {
	int	id = -1;
	TupleDesc tupdesc = nullptr;
	int	keyno[CATCACHE_MAXKEYS];
	int	nkeys = 0;
	Oid	reloid = InvalidOid;
	Oid	indexoid = InvalidOid;

	CCHashFN hashfunc[CATCACHE_MAXKEYS] = {nullptr}; /* hash function for each key */

	// this scan key will use to search tuple that is not exist in cache in future,
	// but now, we not use it
	ScanKeyData skey[CATCACHE_MAXKEYS];
	std::shared_ptr<Cache> catalog_cache = nullptr;
};

public:
	CatalogCache() = default;
	~CatalogCache() = default;

	void InitCatalogCache(SDBContext *cxt);
	void Insert(int cache_id, const Datum* arguments, int size,
	 			HeapTuple tuple, SDBContext *cxt);
	HeapTuple Search(int cache_id, const Datum* arguments,
	 				int size, SDBContext *cxt);
	void DeleteOneKey(int cache_id, const Datum* arguments,
	 				int size, SDBContext *cxt);
	void DeleteOneKey(HeapTuple tuple, SDBContext *cxt);
	void DeleteOneCache(int cache_id, SDBContext *cxt);
	void ClearAll(SDBContext *cxt);
	TupleDesc GetCacheTupleDesc(int cache_id);
	uint32 GetCacheHashValue(int cache_id, Datum key1, Datum key2,
	 				Datum key3, Datum key4);
	bool RelationRelHasSysCache(Oid relid);
	bool RelationIndexHasSysCache(Oid relid);
	int GetCacheIdByIndexOid(Oid index_oid);
	CatCList* GetCatCacheList(int cache_id, const Datum* arguments,
	 						int size, SDBContext *cxt);
	void DeleteOneRelCache(Oid rel_oid, SDBContext *cxt);
protected:
	void InitRelationInfo(CatCacheData *cache, SDBContext *cxt);
	void WriteDatumToString(std::string& str, const Datum* arguments,
	 					int size, CatCacheData* cache);
	HeapTuple MakeCatTuple(int cache_id, const std::string& value);
	void MakeCatCacheKey(HeapTuple tuple, std::string& key);
	HeapTuple SyncFromCatalogRelation(int cache_id, const Datum* arguments,
	 					int size, SDBContext *cxt);
	bool IndexScanOk(int cache_id);
	void CorssCheckTuple(int cache_id, Datum key1, HeapTuple tuple);
private:
	std::vector<CatCacheData> catalog_cache_;

	// we not modify this data after InitCatalogCache.
	// <relation oid, {cache id}>
	std::unordered_map<uint32, std::vector<uint32>> relation_map_;
	// <index oid, cache id>
	std::unordered_map<uint32, uint32> index_map_;
};


}