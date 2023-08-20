
#include <mutex>
_Pragma("once")

#include "cache.hpp"

namespace sdb {

template<typename Key, typename Value>
class NoUseDisk : public Disk<Key, Value> {
public:
	NoUseDisk() = default;
	~NoUseDisk() = default;
    void Put(const Key& key, const Value& value, SDBContext *cxt) {};
    bool Get(const Key& key, Value& value, SDBContext *cxt) {return false;};
    void Delete(const Key& key, SDBContext *cxt) {};
    void Clear(SDBContext *cxt) {};
	void InitDisk(const std::string& path, SDBContext *cxt) {}
};

template<typename Key, typename Value,
 		typename Policy = Policy<Key>,
 		typename Disk = Disk<Key, Value>>
class LocalCache : public Cache<Key, Value> {
private:
	struct CacheValue {
		std::string& GetValue() {
			return value;
		}

		CacheValue(const Value & v, bool dirty): value(v), is_dirty(dirty) {}
		Value value;
		bool is_dirty;
	};
public:
    using Iterator = typename std::unordered_map<Key, std::shared_ptr<CacheValue>>::iterator;

    class LockGuard
    {
    public:

      	explicit LockGuard(LocalCache *cache): cache_(cache) {
			if (cache_) {
				cache_->LockCache();
			}
	  	}

    	~LockGuard() {
			if (cache_) {
				cache_->UnLockCache();
			}
		}

      LockGuard(const LockGuard&) = delete;
      LockGuard& operator=(const LockGuard&) = delete;
	private:
		LocalCache *cache_ = nullptr;
    };
	 
public:
	LocalCache() = default;
	~LocalCache() = default;

    Iterator Begin() {
        return cache_.begin();
    }

    Iterator End() {
        return cache_.end();
    }

	void Init(const std::string &filename, size_t capacity, SDBContext *cxt) {
		disk_.InitDisk(filename, cxt);
		capacity_ = capacity;
	}

    void Put(const Key& key, const Value& value, SDBContext *cxt) override {
        std::lock_guard<std::mutex> lock(mutex_);
        PutCacheKV(key, value, true, cxt);
    }

    bool Get(const Key& key, Value& value, SDBContext *cxt) override {
        {
			std::lock_guard<std::mutex> lock(mutex_);
			auto it = cache_.find(key);
			if (it != cache_.end()) {
				value = (it->second)->value;
				policy_.Touch(key);
				return true;
			}
		}

        if (disk_.Get(key, value, cxt)) {
			std::lock_guard<std::mutex> lock(mutex_);
			PutCacheKV(key, value, false, cxt);
            return true;
        }

        return false;
    }

    void Delete(const Key& key, SDBContext *cxt) override {
		bool is_dirty = true;
        {
			std::lock_guard<std::mutex> lock(mutex_);
			is_dirty = cache_[key]->is_dirty;
        	cache_.erase(key);
		}

		if (!is_dirty) {
        	disk_.Delete(key, cxt);
		}
    }

    void Clear(SDBContext *cxt) override {
		{
        	std::lock_guard<std::mutex> lock(mutex_);
        	cache_.clear();
		}
        disk_.Clear(cxt);
    }

protected:
	void PutCacheKV(const Key& key, const Value& value, bool is_dirty, SDBContext *cxt) {
		if (cache_.size() >= capacity_) {
            std::vector<Key> keysToEvict;
			policy_.Evict(1, keysToEvict);
            for (const auto& evictKey : keysToEvict) {
				auto cache_value = cache_[evictKey];
				if (cache_value->is_dirty) {
                	disk_.Put(evictKey, cache_value->value, cxt);
				}
                cache_.erase(evictKey);
            }
        }

        cache_[key] = std::make_shared<CacheValue>(value, is_dirty);
        policy_.Touch(key);
	}

private:
	void LockCache() {
		mutex_.lock();
	}

	void UnLockCache() {
		mutex_.unlock();
	}
private:
    std::unordered_map<Key, std::shared_ptr<CacheValue>> cache_;
    std::mutex mutex_;
    Policy policy_;
    Disk disk_;
	std::size_t capacity_ = 1000;
};

} // namespace sdb