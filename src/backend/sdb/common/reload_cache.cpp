
extern "C" {
#include "sdb/reload_cache.h"
#include "postgres.h"
#include "catalog/indexing.h"
#include "catalog/index.h"
#include "catalog/pg_class_d.h"
#include "storage/bufmgr.h"
}
#include <vector>
#include "../catalog_index/catalog_to_index.hpp"


extern "C" void
ReloadAllCatalogCacheIndex(void)
{
	auto type_iter = sdb::catalog_to_index.find(TypeRelationId);
	for (const auto& idx_iter : type_iter->second) {
		Oid idx_oid = idx_iter;
		Oid rel_oid = type_iter->first;

		unlink_oid(idx_iter, false);
		unlink_oid(idx_iter, true);
		force_reindex_index(idx_oid, rel_oid, true, RELPERSISTENCE_PERMANENT, 0);
	}
	kInitIndex = IIState_PG_TYPE;

	auto class_iter = sdb::catalog_to_index.find(RelationRelationId);
	for (const auto& idx_iter : class_iter->second) {
		Oid idx_oid = idx_iter;
		Oid rel_oid = class_iter->first;

		unlink_oid(idx_iter, false);
		unlink_oid(idx_iter, true);
		force_reindex_index(idx_oid, rel_oid, true, RELPERSISTENCE_PERMANENT, 0);
	}
	kInitIndex = IIState_PG_CLASS;

	auto attr_iter = sdb::catalog_to_index.find(AttributeRelationId);
	for (const auto& idx_iter : attr_iter->second) {
		Oid idx_oid = idx_iter;
		Oid rel_oid = attr_iter->first;

		unlink_oid(idx_iter, false);
		unlink_oid(idx_iter, true);
		force_reindex_index(idx_oid, rel_oid, true, RELPERSISTENCE_PERMANENT, 0);
	}
	kInitIndex = IIState_PG_ATTR;

	for (auto cat_iter = sdb::catalog_to_index.begin();
		cat_iter != sdb::catalog_to_index.end(); ++ cat_iter) {
		if (cat_iter->first == TypeRelationId ||
			  cat_iter->first == RelationRelationId ||
			  cat_iter->first == AttributeRelationId) {
			continue;
		}

        for (const auto& idx_iter : cat_iter->second) {
			Oid idx_oid = idx_iter;
			Oid rel_oid = cat_iter->first;

			unlink_oid(idx_iter, false);
			unlink_oid(idx_iter, true);
			force_reindex_index(idx_oid, rel_oid, true, RELPERSISTENCE_PERMANENT, 0);
		}
	}

	kInitIndex = IIState_FINISH;
}

extern "C" void
ReloadOneCatalogCacheIndex(int rel_oid)
{
	auto cat_iter = sdb::catalog_to_index.find(rel_oid);
	Assert(cat_iter != sdb::catalog_to_index.end());

	for (const auto& idx_iter : cat_iter->second) {
		Oid idx_oid = idx_iter;
		Oid rel_oid = cat_iter->first;
elog(LOG, "dddtest reload rel:%d - idx:%d", rel_oid, idx_oid);
		unlink_oid(idx_iter, false);
		unlink_oid(idx_iter, true);
		DropOidBuffers(idx_oid);
		force_reindex_index(idx_oid, rel_oid, true, RELPERSISTENCE_PERMANENT, 0);
	}
}
