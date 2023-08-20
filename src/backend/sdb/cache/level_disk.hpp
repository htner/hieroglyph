_Pragma("once")

#include "cache.hpp"

namespace sdb {

template<typename Key, typename Value>
class LevelDBDisk : public Disk<Key, Value> {
private:
    std::unique_ptr<leveldb::DB> db_;

public:
    LevelDBDisk() = default;
	~LevelDBDisk() = default;

	void InitDisk(const std::string& path, SDBContext *cxt) override {
		leveldb::Options options;
        options.create_if_missing = true;
        leveldb::Status status = leveldb::DB::Open(options, path, &db_);
        if (!status.ok()) {
            std::cerr << "Failed to open LevelDB: " << status.ToString() << std::endl;
        }
	}

    ~LevelDBDisk() {
        db_.reset();
    }

    void Put(const Key& key, const Value& value, SDBContext *cxt) override {
        std::string keyStr(reinterpret_cast<const char*>(&key), sizeof(Key));
        std::string valueStr(reinterpret_cast<const char*>(&value), sizeof(Value));
        leveldb::Status status = db_->Put(leveldb::WriteOptions(), keyStr, valueStr);
        if (!status.ok()) {
            std::cerr << "Failed to put data into LevelDB: " << status.ToString() << std::endl;
        }
    }

    bool Get(const Key& key, Value& value, SDBContext *cxt) override {
        std::string keyStr(reinterpret_cast<const char*>(&key), sizeof(Key));
        std::string valueStr;
        leveldb::Status status = db_->Get(leveldb::ReadOptions(), keyStr, &valueStr);
        if (status.IsNotFound()) {
            return false;
        }
        if (!status.ok()) {
            std::cerr << "Failed to get data from LevelDB: " << status.ToString() << std::endl;
            return false;
        }
        if (valueStr.size() != sizeof(Value)) {
            std::cerr << "Data size mismatch in LevelDB" << std::endl;
            return false;
        }
        value = *reinterpret_cast<const Value*>(valueStr.data());
        return true;
    }

    void Delete(const Key& key, SDBContext *cxt) override {
        std::string keyStr(reinterpret_cast<const char*>(&key), sizeof(Key));
        leveldb::Status status = db_->Delete(leveldb::WriteOptions(), keyStr);
        if (!status.ok()) {
            std::cerr << "Failed to delete data from LevelDB: " << status.ToString() << std::endl;
        }
    }

    void Clear(SDBContext *cxt) override {
        leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            leveldb::Status status = db_->Delete(leveldb::WriteOptions(), it->key());
            if (!status.ok()) {
                std::cerr << "Failed to delete data from LevelDB: " << status.ToString() << std::endl;
            }
        }
        if (!it->status().ok()) {
            std::cerr << "LevelDB iteration error: " << it->status().ToString() << std::endl;
        }
        delete it;
    }
};

} // namespace sdb