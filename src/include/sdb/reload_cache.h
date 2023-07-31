/*-------------------------------------------------------------------------
 *
 * reload_cache.h
 *
 * src/include/sdb/reload_cache.h
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef SDB_RELOAD_CACHE_H
#define SDB_RELOAD_CACHE_H

extern void ReloadAllCatalogCacheIndex(void);
extern void ReloadOneCatalogCacheIndex(int rel_oid);

#endif