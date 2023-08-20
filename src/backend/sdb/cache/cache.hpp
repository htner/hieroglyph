_Pragma("once")

#include <iostream>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <list>
#include "../common/context.hpp"
#include <leveldb/db.h>

namespace sdb {

template<typename Key, typename Value>
class Cache {
public:
    virtual void Put(const Key& key, const Value& value, SDBContext *cxt) = 0;
    virtual bool Get(const Key& key, Value& value, SDBContext *cxt) = 0;
    virtual void Delete(const Key& key, SDBContext *cxt) = 0;
    virtual void Clear(SDBContext *cxt) = 0;
	virtual ~Cache() = default;
};

template<typename Key>
class Policy {
public:
    virtual void Touch(const Key& key) = 0;
	virtual void Insert(const Key& numKeys) = 0;
    virtual void Evict(size_t numKeys, std::vector<Key>& keys) = 0;
	virtual ~Policy() = default;
};

template<typename Key, typename Value>
class Disk {
public:
    virtual void Put(const Key& key, const Value& value, SDBContext *cxt) = 0;
    virtual bool Get(const Key& key, Value& value, SDBContext *cxt) = 0;
    virtual void Delete(const Key& key, SDBContext *cxt) = 0;
    virtual void Clear(SDBContext *cxt) = 0;
	virtual void InitDisk(const std::string& path, SDBContext *cxt) = 0;
	virtual ~Disk() = default;
};

template<typename Key>
class LRUPolicy : public Policy<Key> {
public:
	LRUPolicy() = default;
	~LRUPolicy() = default;

	void Touch(const Key& key) override {
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            order_.splice(order_.begin(), order_, it->second);
        }
    }

    void Evict(size_t numKeys, std::vector<Key>& keys) override {
		for (size_t i = 0; i < numKeys; ++i) {
			Key evictedKey = order_.back();
			cache_.erase(evictedKey);
			order_.pop_back();
			keys.emplace_back(std::move(evictedKey));
		}
    }

    void Insert(const Key& key) override {
        order_.push_front(key);
        cache_[key] = order_.begin();
    }

private:
    std::unordered_map<Key, typename std::list<Key>::iterator> cache_;
    std::list<Key> order_;
};


}  // namespace sdb