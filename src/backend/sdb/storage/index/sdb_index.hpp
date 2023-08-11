
#pragma once

#include <unordered_map>
#include <string>
#include <mutex>

namesapce sdb {

struct IndexValue {
	std::mutex mtx_;
	std::unordered_map<std::string, uint64_t> value;
};

class SDBIndex {
public:
	SDBIndex() = default;
	~SDBIndex() = default;

	void IndexCacheInsert(uint64_t rel_oid, const std::string &key, uint64_t tid);
	void IndexCacheRelationDelete(uint64_t rel_oid);
	void IndexCacheGetTuple(uint64_t oid);
	void IndexCacheAllInvalid();

private:
	// <relation oid, <index key, tuple tid>>
	std::unordered_map<uint64_t, IndexValue> index_cache_;
	std::mutex mtx_;
};

} // namesapce sdb