
extern "C" {
#include "c.h"
#include "postgres.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "utils/rel.h"
#include "utils/catcache.h"
#include "utils/syscache.h"
#include "access/amapi.h"
#include "utils/datum.h"
#include "access/tupdesc.h"
#include "nodes/pg_list.h"
#include "access/tuptoaster.h"
#include "access/relscan.h"
#include "access/genam.h"
#include "storage/lockdefs.h"
#include "miscadmin.h"
#include "access/htup.h"
}

#include "local_cache.hpp"
#include "../common/singleton.hpp"
#include "sdb_catalog_cache.hpp"
#include "sdb/sdb_context.h"
#include <thread>

constexpr uint32 CACHE_CAP = 100000;

static HeapTuple SearchSDBCatalogCache(int cache_id, Datum v1, Datum v2, Datum v3,
					   			Datum v4, int size, SDBContext *cxt) {
	sdb::SDBContext* sdb_cxt = (sdb::SDBContext*)cxt;
	Datum arguments[CATCACHE_MAXKEYS];
	arguments[0] = v1;
	arguments[1] = v2;
	arguments[2] = v3;
	arguments[3] = v4;

	return ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()->Search(cache_id,
															arguments, size, sdb_cxt);
}

void InitCatalogCache(void) {
	if (!CacheMemoryContext) {
		CreateCacheMemoryContext();
	}
	ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()->InitCatalogCache(nullptr);
}

void InitCatalogCachePhase2(void) {
	return;
}

HeapTuple SearchSysCache(int cacheId, Datum key1, Datum key2,
 						 Datum key3, Datum key4) {
	sdb::SDBContext* sdb_cxt = nullptr;
	Datum arguments[CATCACHE_MAXKEYS];
	arguments[0] = key1;
	arguments[1] = key2;
	arguments[2] = key3;
	arguments[3] = key4;

	return ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
					->Search(cacheId, arguments, sdb_cacheinfo[cacheId].nkeys, sdb_cxt);
}

HeapTuple SearchSysCache1(int cacheId, Datum key1) {
	return SearchSDBCatalogCache(cacheId, key1, 0, 0, 0, 1, nullptr);
}
HeapTuple SearchSysCache2(int cacheId, Datum key1, Datum key2) {
    return SearchSDBCatalogCache(cacheId, key1, key2, 0, 0, 2, nullptr);
}
HeapTuple SearchSysCache3(int cacheId, Datum key1, Datum key2,
 							Datum key3) {
	return SearchSDBCatalogCache(cacheId, key1, key2, key3, 0, 3, nullptr);								
}
HeapTuple SearchSysCache4(int cacheId, Datum key1, Datum key2,
 							Datum key3, Datum key4) {
    return SearchSDBCatalogCache(cacheId, key1, key2, key3, key4, 4, nullptr);	
}

void ReleaseSysCache(HeapTuple tuple) {
	if (tuple) {
		pfree(tuple);
	}
	/* FIXME: we not flust catalog to disk, so we dont remove this cache */
	return;
	sdb::SDBContext* sdb_cxt = nullptr;
	ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()->DeleteOneKey(tuple, sdb_cxt);
	
}

void ReleaseCatCache(HeapTuple tuple) {
	ReleaseSysCache(tuple);
}

/*
 * SearchSysCacheCopy
 *
 * A convenience routine that does SearchSysCache and (if successful)
 * returns a modifiable copy of the syscache entry.  The original
 * syscache entry is released before returning.  The caller should
 * heap_freetuple() the result when done with it.
 */
HeapTuple SearchSysCacheCopy(int cacheId, Datum key1, Datum key2,
 								Datum key3, Datum key4) {
    HeapTuple	tuple,
				newtuple;
	tuple = SearchSysCache(cacheId, key1, key2, key3, key4);
	if (!HeapTupleIsValid(tuple))
		return tuple;
	newtuple = heap_copytuple(tuple);
	ReleaseSysCache(tuple);
	return newtuple;
}

/*
 * SearchSysCacheExists
 *
 * A convenience routine that just probes to see if a tuple can be found.
 * No lock is retained on the syscache entry.
 */
bool SearchSysCacheExists(int cacheId, Datum key1, Datum key2,
 							Datum key3, Datum key4) {
	HeapTuple	tuple;

	tuple = SearchSysCache(cacheId, key1, key2, key3, key4);
	if (!HeapTupleIsValid(tuple))
		return false;
	ReleaseSysCache(tuple);
	return true;
}

/*
 * GetSysCacheOid
 *
 * A convenience routine that does SearchSysCache and returns the OID in the
 * oidcol column of the found tuple, or InvalidOid if no tuple could be found.
 * No lock is retained on the syscache entry.
 */
Oid	GetSysCacheOid(int cacheId, AttrNumber oidcol, Datum key1,
 							Datum key2, Datum key3, Datum key4) {

	HeapTuple	tuple;
	bool		isNull;
	Oid			result;
	TupleDesc   tupdesc;

	tuple = SearchSysCache(cacheId, key1, key2, key3, key4);
	if (!HeapTupleIsValid(tuple))
		return InvalidOid;
	
	tupdesc = ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
								->GetCacheTupleDesc(cacheId);

	result = heap_getattr(tuple, oidcol,
						  tupdesc,
						  &isNull);
	Assert(!isNull);			/* columns used as oids should never be NULL */
	ReleaseSysCache(tuple);
	return result;
}

/*
 * SearchSysCacheAttName
 *
 * This routine is equivalent to SearchSysCache on the ATTNAME cache,
 * except that it will return NULL if the found attribute is marked
 * attisdropped.  This is convenient for callers that want to act as
 * though dropped attributes don't exist.
 */
HeapTuple SearchSysCacheAttName(Oid relid, const char *attname) {
	HeapTuple	tuple;

	tuple = SearchSysCache2(ATTNAME,
							ObjectIdGetDatum(relid),
							CStringGetDatum(attname));
	if (!HeapTupleIsValid(tuple))
		return NULL;
	if (((Form_pg_attribute) GETSTRUCT(tuple))->attisdropped)
	{
		ReleaseSysCache(tuple);
		return NULL;
	}
	return tuple;
}

/*
 * SearchSysCacheCopyAttName
 *
 * As above, an attisdropped-aware version of SearchSysCacheCopy.
 */
HeapTuple SearchSysCacheCopyAttName(Oid relid, const char *attname) {
	HeapTuple	tuple,
				newtuple;

	tuple = SearchSysCacheAttName(relid, attname);
	if (!HeapTupleIsValid(tuple))
		return tuple;
	newtuple = heap_copytuple(tuple);
	ReleaseSysCache(tuple);
	return newtuple;
}

/*
 * SearchSysCacheExistsAttName
 *
 * As above, an attisdropped-aware version of SearchSysCacheExists.
 */
bool SearchSysCacheExistsAttName(Oid relid, const char *attname) {
	HeapTuple	tuple;

	tuple = SearchSysCacheAttName(relid, attname);
	if (!HeapTupleIsValid(tuple))
		return false;
	ReleaseSysCache(tuple);
	return true;
}

/*
 * SearchSysCacheAttNum
 *
 * This routine is equivalent to SearchSysCache on the ATTNUM cache,
 * except that it will return NULL if the found attribute is marked
 * attisdropped.  This is convenient for callers that want to act as
 * though dropped attributes don't exist.
 */
HeapTuple SearchSysCacheAttNum(Oid relid, int16 attnum) {
	HeapTuple	tuple;

	tuple = SearchSysCache2(ATTNUM,
							ObjectIdGetDatum(relid),
							Int16GetDatum(attnum));
	if (!HeapTupleIsValid(tuple))
		return NULL;
	if (((Form_pg_attribute) GETSTRUCT(tuple))->attisdropped)
	{
		ReleaseSysCache(tuple);
		return NULL;
	}
	return tuple;
}

/*
 * SearchSysCacheCopyAttNum
 *
 * As above, an attisdropped-aware version of SearchSysCacheCopy.
 */
HeapTuple SearchSysCacheCopyAttNum(Oid relid, int16 attnum) {
	HeapTuple	tuple,
				newtuple;

	tuple = SearchSysCacheAttNum(relid, attnum);
	if (!HeapTupleIsValid(tuple))
		return NULL;
	newtuple = heap_copytuple(tuple);
	ReleaseSysCache(tuple);
	return newtuple;
}

/*
 * SysCacheGetAttr
 *
 *		Given a tuple previously fetched by SearchSysCache(),
 *		extract a specific attribute.
 *
 * This is equivalent to using heap_getattr() on a tuple fetched
 * from a non-cached relation.  Usually, this is only used for attributes
 * that could be NULL or variable length; the fixed-size attributes in
 * a system table are accessed just by mapping the tuple onto the C struct
 * declarations from include/catalog/.
 *
 * As with heap_getattr(), if the attribute is of a pass-by-reference type
 * then a pointer into the tuple data area is returned --- the caller must
 * not modify or pfree the datum!
 *
 * Note: it is legal to use SysCacheGetAttr() with a cacheId referencing
 * a different cache for the same catalog the tuple was fetched from.
 */
Datum SysCacheGetAttr(int cacheId, HeapTuple tup, AttrNumber attributeNumber,
 								bool *isNull) {
	TupleDesc tupdesc = ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
										->GetCacheTupleDesc(cacheId);

	return heap_getattr(tup, attributeNumber, tupdesc, isNull);
}


/*
 * GetSysCacheHashValue
 *
 * Get the hash value that would be used for a tuple in the specified cache
 * with the given search keys.
 *
 * The reason for exposing this as part of the API is that the hash value is
 * exposed in cache invalidation operations, so there are places outside the
 * catcache code that need to be able to compute the hash values.
 */
uint32 GetSysCacheHashValue(int cacheId, Datum key1, Datum key2,
 								Datum key3, Datum key4) {
	return ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
					->GetCacheHashValue(cacheId, key1, key2, key3, key4);
}

struct catclist *SearchSysCacheList(int cacheId, int nkeys, Datum key1,
 									Datum key2, Datum key3) {
	sdb::SDBContext* sdb_cxt = nullptr;
	Datum arguments[CATCACHE_MAXKEYS];
	arguments[0] = key1;
	arguments[1] = key2;
	arguments[2] = key3;
	return ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
				->GetCatCacheList(cacheId, arguments, nkeys, sdb_cxt);
}

void ReleaseCatCacheList(CatCList *list) {
	if (list) {
		pfree(list);
	}
}

void ResetCatalogCaches(void) {
	sdb::SDBContext* sdb_cxt = nullptr;
	return ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
						->ClearAll(sdb_cxt);
}
void CatalogCacheFlushCatalog(Oid catId) { 
	sdb::SDBContext* sdb_cxt = nullptr;
	return ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
				->DeleteOneCache(catId, sdb_cxt);
}

void CatalogCacheInvalidRelCatalog(Oid rel_oid) {
	sdb::SDBContext* sdb_cxt = nullptr;
	return ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
				->DeleteOneRelCache(rel_oid, sdb_cxt);
}

void PrepareToInvalidateCacheTuple(Relation relation,
										  HeapTuple tuple,
										  HeapTuple newtuple,
										  void (*function) (int, uint32, Oid)) {}
void PrintCatCacheLeakWarning(HeapTuple tuple, const char *resOwnerName) {}
void PrintCatCacheListLeakWarning(CatCList *list, const char *resOwnerName) {}
/*
 * GetSysCacheHashValue
 *
 * Get the hash value that would be used for a tuple in the specified cache
 * with the given search keys.
 *
 * The reason for exposing this as part of the API is that the hash value is
 * exposed in cache invalidation operations, so there are places outside the
 * catcache code that need to be able to compute the hash values.
 */
void SysCacheInvalidate(int cacheId, uint32 hashValue) {
	return;
}

/*
 * Certain relations that do not have system caches send snapshot invalidation
 * messages in lieu of catcache messages.  This is for the benefit of
 * GetCatalogSnapshot(), which can then reuse its existing MVCC snapshot
 * for scanning one of those catalogs, rather than taking a new one, if no
 * invalidation has been received.
 *
 * Relations that have syscaches need not (and must not) be listed here.  The
 * catcache invalidation messages will also flush the snapshot.  If you add a
 * syscache for one of these relations, remove it from this list.
 */
bool RelationInvalidatesSnapshotsOnly(Oid relid) {
	switch (relid)
	{
		case DbRoleSettingRelationId:
		case DependRelationId:
		case SharedDependRelationId:
		case DescriptionRelationId:
		case SharedDescriptionRelationId:
		case SecLabelRelationId:
		case SharedSecLabelRelationId:
			return true;
		default:
			break;
	}

	return false;
}
bool RelationHasSysCache(Oid relid) {
	return false;
}
bool RelationSupportsSysCache(Oid relid) {
	return ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
									->RelationIndexHasSysCache(relid);
}

void SysOneCacheInvalidate(int idx_oid) {
	int cache_id = ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
								->GetCacheIdByIndexOid(idx_oid);
	if (cache_id < 0) {
		return;
	}

	ThreadSafeSingleton<sdb::CatalogCache>::GetInstance()
								->DeleteOneCache(cache_id, nullptr);
}

// sdb catalog cache
namespace sdb {

void CatalogCache::InitCatalogCache(SDBContext *cxt) {
	for (int i = 0; i < SysCacheSize; ++i) {
		CatCacheData data;
		data.id = i;
		data.reloid = sdb_cacheinfo[i].reloid;
		data.indexoid = sdb_cacheinfo[i].indoid;
		data.nkeys = sdb_cacheinfo[i].nkeys;
		relation_map_[data.reloid].push_back(i);
		memset(data.skey, 0, sizeof(data.skey));
		index_map_.insert({data.indexoid, i});
		for (int j = 0; j < data.nkeys; ++j) {
			data.keyno[j] = sdb_cacheinfo[i].key[j];
		}
		data.catalog_cache = std::make_shared<Cache>();
		data.catalog_cache->Init("", CACHE_CAP, cxt);
		catalog_cache_.emplace_back(data);
	}

	if (IsBootstrapProcessingMode()) {
		return;
	}

	for (int i = 0; i < SysCacheSize; ++i) {
		InitRelationInfo(&catalog_cache_[i], cxt);
	}
}

void CatalogCache::InitRelationInfo(CatCacheData *cache, SDBContext *cxt) {
	Relation	relation;
	MemoryContext oldcxt;

	relation = table_open(cache->reloid, NoLock);

	/*
	 * switch to the cache context so our allocations do not vanish at the end
	 * of a transaction
	 */
	Assert(CacheMemoryContext != NULL);

	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
	cache->tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(relation));

	/*
	 * return to the caller's memory context and close the rel
	 */
	MemoryContextSwitchTo(oldcxt);
	table_close(relation, NoLock);

	for (int i = 0; i < cache->nkeys; ++i) {
		Oid	keytype = InvalidOid;
		RegProcedure eqfunc;
		CCFastEqualFN fasteqfunc;
		if (cache->keyno[i] > 0) {
			Form_pg_attribute attr = TupleDescAttr(cache->tupdesc,
												   cache->keyno[i] - 1);
			keytype = attr->atttypid;
		} else {
			if (cache->keyno[i] < 0) {
				elog(FATAL, "sys attributes are not supported in caches");
				keytype = OIDOID;
			}
		}

		GetCCHashEqFuncs(keytype, &cache->hashfunc[i], &eqfunc, &fasteqfunc);

		/*
		 * Do equality-function lookup (we assume this won't need a catalog
		 * lookup for any supported type)
		 */
		fmgr_info_cxt(eqfunc,
					  &cache->skey[i].sk_func,
					  CacheMemoryContext);

		/* Initialize sk_attno suitably for HeapKeyTest() and heap scans */
		cache->skey[i].sk_attno = cache->keyno[i];

		/* Fill in sk_strategy as well --- always standard equality */
		cache->skey[i].sk_strategy = BTEqualStrategyNumber;
		cache->skey[i].sk_subtype = InvalidOid;
		/* If a catcache key requires a collation, it must be C collation */
		cache->skey[i].sk_collation = C_COLLATION_OID;
	}
}

void CatalogCache::Insert(int cache_id, const Datum* arguments, int size,
	 					 HeapTuple tuple, SDBContext *cxt) {
	
	Assert(catalog_cache_.size() < cache_id);

	auto& cache = catalog_cache_[cache_id];

	// We only insert one cache after init. We also not clear relation info
	Assert(cache.tupdesc != nullptr);

	std::string key("");
	std::string value("");
	WriteDatumToString(key, arguments, size, &cache);
	value.append((char *)tuple->t_data, tuple->t_len);
	cache.catalog_cache->Put(key, value, cxt);
}

HeapTuple CatalogCache::Search(int cache_id, const Datum* arguments, int size,
 								SDBContext *cxt) {
	std::string value;
	HeapTuple tuple = nullptr;
	std::string key;

	//std::this_thread::sleep_for(std::chrono::minutes(1));
	Assert(catalog_cache_.size() < cache_id);

	auto& cache = catalog_cache_[cache_id];

	if (cache.tupdesc == nullptr) {
		InitRelationInfo(&cache, cxt);
	}

	// We only insert one cache after init. We also not clear relation info
	Assert(cache.tupdesc != nullptr);

	WriteDatumToString(key, arguments, size, &cache);

	if (cache.catalog_cache->Get(key, value, cxt)) {
		tuple = MakeCatTuple(cache_id, value);
	} else {
		tuple = SyncFromCatalogRelation(cache_id, arguments, size, cxt);
	}

	return tuple;
}

void CatalogCache::DeleteOneKey(int cache_id, const Datum* arguments, int size,
 								 SDBContext *cxt) {
    Assert(catalog_cache_.size() < cache_id);

	auto& cache = catalog_cache_[cache_id];

	// We only insert one cache after init. We also not clear relation info
	Assert(cache.tupdesc != nullptr);

	std::string key;
	WriteDatumToString(key, arguments, size, &cache);
	cache.catalog_cache->Delete(key, cxt);
}

void CatalogCache::DeleteOneKey(HeapTuple tuple, SDBContext *cxt) {

	int cache_id = BlockIdGetBlockNumber(&tuple->t_self.ip_blkid);
	Assert(catalog_cache_.size() < cache_id);

	auto& cache = catalog_cache_[cache_id];

	// We only insert one cache after init. We also not clear relation info
	Assert(cache.tupdesc != nullptr);
	std::string key;
	MakeCatCacheKey(tuple, key);
	cache.catalog_cache->Delete(key, cxt);
}

void CatalogCache::DeleteOneCache(int cache_id, SDBContext *cxt) {
	Assert(catalog_cache_.size() < cache_id);

	auto& cache = catalog_cache_[cache_id];

	// We only insert one cache after init. We also not clear relation info
	Assert(cache.tupdesc != nullptr);

	cache.catalog_cache->Clear(cxt);
}

void CatalogCache::DeleteOneRelCache(Oid rel_oid, SDBContext *cxt) {
	auto iter = relation_map_.find(rel_oid);
	if (iter == relation_map_.end()) {
		return;
	}

	for (auto cat_id : iter->second) {
		this->DeleteOneCache(cat_id, cxt);
	}
}

void CatalogCache::ClearAll(SDBContext *cxt) {
	for (auto& cache : catalog_cache_) {
		cache.catalog_cache->Clear(cxt);
	}
}

void CatalogCache::WriteDatumToString(std::string& str, const Datum* arguments, int size,
 										CatCacheData* cache)
{
	size_t length = 0;
	char *s = nullptr;
	TupleDesc tupdesc = cache->tupdesc;

	for (int i = 0; i < size; ++i) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, cache->keyno[i] - 1);

		if (attr->attbyval) {
			s = (char *) (&arguments[i]);
			str.append(s, sizeof(Datum));
		} else {
			s = (char *) DatumGetPointer(arguments[i]);
			if (PointerIsValid(s)) {
				if (attr->attlen == -1 && VARATT_IS_EXTERNAL_EXPANDED(s)) {
					ExpandedObjectHeader *eoh = DatumGetEOHP(arguments[i]);
					size_t resultsize;
					char *resultptr;

					resultsize = EOH_get_flat_size(eoh);
					resultptr = (char *) palloc(resultsize);
					EOH_flatten_into(eoh, (void *) resultptr, resultsize);
					str.append(resultptr, resultsize);
				} else {
					/* namedata column, so we should get len by strlen. datumGetSize will
					 * return NAMEDATALEN, but the len of Datum is not equal NAMEDATALEN
					 */
					if (attr->attlen == NAMEDATALEN) {
						str.append(s);
					} else {
						length = datumGetSize(arguments[i], attr->attbyval, attr->attlen);
						str.append(s, length);
					}
				}
			}
		}
	}
}

HeapTuple CatalogCache::MakeCatTuple(int cache_id, const std::string& value) {
	HeapTuple tuple = nullptr;
	MemoryContext oldcxt;
	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	tuple = (HeapTuple) palloc(HEAPTUPLESIZE);
	tuple->t_len = value.size();
	tuple->t_data = (HeapTupleHeader) palloc(tuple->t_len);
	memcpy(tuple->t_data, value.data(), tuple->t_len);
	// we use t_self store cache id
	BlockIdSet(&tuple->t_self.ip_blkid, (uint64)cache_id);
	tuple->t_tableOid = catalog_cache_[cache_id].reloid;
	MemoryContextSwitchTo(oldcxt);
	return tuple;
}

void CatalogCache::MakeCatCacheKey(HeapTuple tuple, std::string& key) {
	int cache_id = BlockIdGetBlockNumber(&tuple->t_self.ip_blkid);

	Assert(catalog_cache_.size() < cache_id);

	auto& cache = catalog_cache_[cache_id];

	// We only insert one cache after init. We also not clear relation info
	Assert(cache.tupdesc != nullptr);
	Datum	keys[CATCACHE_MAXKEYS];
	for (int i = 0; i < cache.nkeys; i++) {
		Datum		atp;
		bool		isnull;

		atp = heap_getattr(tuple,
							cache.keyno[i],
							cache.tupdesc,
							&isnull);
		Assert(!isnull);
		keys[i] = atp;
	}
	WriteDatumToString(key, keys, cache.nkeys, &cache);
}

TupleDesc CatalogCache::GetCacheTupleDesc(int cache_id) {
	Assert(catalog_cache_.size() < cache_id);

	auto& cache = catalog_cache_[cache_id];

	// We only insert one cache after init. We also not clear relation info
	Assert(cache.tupdesc != nullptr);

	return cache.tupdesc;
}

uint32 CatalogCache::GetCacheHashValue(int cache_id, Datum key1, Datum key2,
	 							Datum key3, Datum key4) {
	Assert(catalog_cache_.size() < cache_id);

	auto& cache = catalog_cache_[cache_id];

	// We only insert one cache after init. We also not clear relation info
	Assert(cache.tupdesc != nullptr);

	uint32		hashValue = 0;
	uint32		oneHash;
	CCHashFN   *cc_hashfunc = cache.hashfunc;

	switch (cache.nkeys) {
		case 4:
			oneHash = (cc_hashfunc[3]) (key4);

			hashValue ^= oneHash << 24;
			hashValue ^= oneHash >> 8;
			/* FALLTHROUGH */
		case 3:
			oneHash = (cc_hashfunc[2]) (key3);

			hashValue ^= oneHash << 16;
			hashValue ^= oneHash >> 16;
			/* FALLTHROUGH */
		case 2:
			oneHash = (cc_hashfunc[1]) (key2);

			hashValue ^= oneHash << 8;
			hashValue ^= oneHash >> 24;
			/* FALLTHROUGH */
		case 1:
			oneHash = (cc_hashfunc[0]) (key1);

			hashValue ^= oneHash;
			break;
		default:
			elog(FATAL, "wrong number of hash keys: %d", cache.nkeys);
			break;
	}

	return hashValue;
}

bool CatalogCache::RelationRelHasSysCache(Oid relid) {
	return (relation_map_.find(relid) != relation_map_.end());
}

bool CatalogCache::RelationIndexHasSysCache(Oid relid) {
	return (index_map_.find(relid) != index_map_.end());
}

CatCList* CatalogCache::GetCatCacheList(int cache_id, const Datum* arguments,
 							int size, SDBContext *cxt) {
	MemoryContext oldcxt;
	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
	Assert(catalog_cache_.size() < cache_id);

	auto& cache = catalog_cache_[cache_id];

	// We only insert one cache after init. We also not clear relation info
	Assert(cache.tupdesc != nullptr);
	List * ctlist = nullptr;
	CatCList  *cl;
	std::string key;
	WriteDatumToString(key, arguments, size, &cache);

	Cache::LockGuard lock(cache.catalog_cache.get());

	for (Cache::Iterator iter = cache.catalog_cache->Begin();
	 			iter != cache.catalog_cache->End(); ++iter) {
		Assert(iter->first.size() >= key.size());
		if (iter->first.compare(0, key.size(), key) != 0) {
			continue;
		}
		std::string& v = iter->second->GetValue();
		HeapTuple tuple = MakeCatTuple(cache_id, v);
		HeapTuple	dtp;
		CatCTup    *ct;
		if (HeapTupleHasExternal(tuple)) {
			dtp = toast_flatten_tuple(tuple, cache.tupdesc);
		} else {
			dtp = tuple;
		}
		
		ct = (CatCTup *) palloc(sizeof(CatCTup) +
								MAXIMUM_ALIGNOF + dtp->t_len);
		ct->tuple.t_len = dtp->t_len;
		ct->tuple.t_self = dtp->t_self;
		ct->tuple.t_tableOid = dtp->t_tableOid;
		ct->tuple.t_data = (HeapTupleHeader)
			MAXALIGN(((char *) ct) + sizeof(CatCTup));
		/* copy tuple contents */
		memcpy((char *) ct->tuple.t_data,
			   (const char *) dtp->t_data,
			   dtp->t_len);

		ctlist = lappend(ctlist, ct);
		if (tuple) {
			pfree(tuple);
		}
		if (dtp) {
			pfree(dtp);
		}
	}

	/* Now we can build the CatCList entry. */
	int nmembers = list_length(ctlist);
	cl = (CatCList *)
		palloc(offsetof(CatCList, members) + nmembers * sizeof(CatCTup *));
	MemoryContextSwitchTo(oldcxt);

	cl->cl_magic = CL_MAGIC;
	cl->my_cache = nullptr;
	cl->refcount = 0;			/* for the moment */
	cl->dead = false;
	cl->ordered = false;
	cl->nkeys = cache.nkeys;
	cl->hash_value = 0;
	cl->n_members = nmembers;

	int i = 0;
	ListCell   *ctlist_item;
	CatCTup    *ct;
	foreach(ctlist_item, ctlist)
	{
		cl->members[i++] = ct = (CatCTup *) lfirst(ctlist_item);
		Assert(ct->c_list == NULL);
		ct->c_list = cl;
	}
	Assert(i == nmembers);

	return cl;
}

int CatalogCache::GetCacheIdByIndexOid(Oid index_oid) {
	auto iter = index_map_.find(index_oid);
	return iter != index_map_.end() ? iter->second : -1;
}

bool CatalogCache::IndexScanOk(int cache_id) {
	switch (cache_id) {
		case AMOID:
		case AMNAME:
			return false;
		default:
			break;
	}

	/* Normal case, allow index scan */
	return true;
}

void CatalogCache::CorssCheckTuple(int cache_id, Datum key1,
 									HeapTuple tuple) {
    Form_pg_class rd_rel;
	Form_pg_type rd_type;

	switch (cache_id)
	{
		case RELOID:
			rd_rel = (Form_pg_class) GETSTRUCT(tuple);
			if (rd_rel->oid != DatumGetObjectId(key1))
			{
				elog(PANIC, "pg_class_oid_index is broken, oid=%d is pointing to tuple with oid=%d (xmin:%u xmax:%u)",
					 DatumGetObjectId(key1), rd_rel->oid,
					 HeapTupleHeaderGetXmin((tuple)->t_data),
					 HeapTupleHeaderGetRawXmax((tuple)->t_data));
			}
			break;
		case RELNAMENSP:
			rd_rel = (Form_pg_class) GETSTRUCT(tuple);
			if (strncmp(rd_rel->relname.data, DatumGetCString(key1), NAMEDATALEN) != 0)
			{
				elog(ERROR, "pg_class_relname_nsp_index is broken, intended tuple with name \"%s\" fetched \"%s\""
					 " (xmin:%u xmax:%u)",
					 DatumGetCString(key1), rd_rel->relname.data,
					 HeapTupleHeaderGetXmin((tuple)->t_data),
					 HeapTupleHeaderGetRawXmax((tuple)->t_data));
			}
			break;
		case TYPEOID:
			rd_type = (Form_pg_type) GETSTRUCT(tuple);
			if (rd_type->oid != DatumGetObjectId(key1))
			{
				elog(ERROR, "pg_type_oid_index is broken, oid=%d is pointing to tuple with oid=%d (xmin:%u xmax:%u)",
					 DatumGetObjectId(key1), rd_type->oid,
					 HeapTupleHeaderGetXmin((tuple)->t_data),
					 HeapTupleHeaderGetRawXmax((tuple)->t_data));
			}
			break;
	}
}

HeapTuple CatalogCache::SyncFromCatalogRelation(int cache_id,
 					const Datum* arguments, int size, SDBContext *cxt) {
    Assert(catalog_cache_.size() < cache_id);

	auto& cache = catalog_cache_[cache_id];

	// We only insert one cache after init. We also not clear relation info
	Assert(cache.tupdesc != nullptr);
	Assert(cache.nkeys == size);

	ScanKeyData cur_skey[CATCACHE_MAXKEYS];
	Relation	relation;
	SysScanDesc scandesc;
	HeapTuple	ntp = nullptr;
	HeapTuple   cache_tp = nullptr;
	MemoryContext oldcxt = nullptr;

	memcpy(cur_skey, cache.skey, sizeof(ScanKeyData) * size);
	cur_skey[0].sk_argument = arguments[0];
	cur_skey[1].sk_argument = arguments[1];
	cur_skey[2].sk_argument = arguments[2];
	cur_skey[3].sk_argument = arguments[3];

	/*
	 * Tuple was not found in cache, so we have to try to retrieve it directly
	 * from the relation.  If found, we will add it to the cache; if not
	 * found, we will add a negative cache entry instead.
	 */
	relation = table_open(cache.reloid, NoLock);

	scandesc = systable_beginscan(relation,
	  						      cache.indexoid,
								  IndexScanOk(cache_id),
								  nullptr,
								  size,
								  cur_skey);
	
	while (HeapTupleIsValid(ntp = systable_getnext(scandesc)))
	{
		/*
		 * Good place to sanity check the tuple, before adding it to cache.
		 * So if its fetched using index, lets cross verify tuple intended is the tuple
		 * fetched. If not fail and contain the damage which maybe caused due to
		 * index corruption for some reason.
		 */
		if (scandesc->irel) {
			CorssCheckTuple(cache_id, arguments[0], ntp);
		}
		oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
		
		cache_tp = (HeapTuple) palloc(HEAPTUPLESIZE);
		heap_copytuple_with_tuple(ntp, cache_tp);
		MemoryContextSwitchTo(oldcxt);

		// we use t_self store cache id
		BlockIdSet(&cache_tp->t_self.ip_blkid, (uint64)cache_id);
		this->Insert(cache_id, arguments, size, cache_tp, cxt);
		cache_tp->t_tableOid = catalog_cache_[cache_id].reloid;
		break;					/* assume only one match */
	}

	systable_endscan(scandesc);
	table_close(relation, NoLock);
	return cache_tp;
}

} // namespace sdb
