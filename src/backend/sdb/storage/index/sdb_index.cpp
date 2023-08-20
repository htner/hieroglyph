
#include "sdb_index.hpp"
#include <mutex>

namespace sdb {

void SDBIndex::IndexCacheInsert(uint64_t rel_oid, const std::string &key, uint64_t tid) {
	std::lock_guard<std::mutex> lock(mtx_);
	auto iter = index_cache_.find(rel_oid);
	if (iter == index_cache_.end()) {
		
	}
}

void SDBIndex::IndexCacheRelationDelete(uint64_t rel_oid) {

}

void SDBIndex::IndexCacheGetTuple(uint64_t oid) {

}

void SDBIndex::IndexCacheAllInvalid() {

}

} // namesapce sdb