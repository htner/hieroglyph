#pragma once

#include "backend/sdb/common/pg_export.hpp"
#include "kvpair.pb.h"
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <mutex>

/*
extern Oid MyDatabaseId;
extern Oid MyDatabaseTableSpace;

extern std::string kDBBucket;
extern std::string kDBS3User;
extern std::string kDBS3Password;
extern std::string kDBS3Region;
extern std::string kDBS3Endpoint;
extern bool kDBIsMinio;

extern std::string kResultBucket;
extern std::string kResultS3User;
extern std::string kResultS3Password;
extern std::string kResultS3Region;
extern std::string kResultS3Endpoint;
extern bool kResultIsMinio;

extern uint64_t dbid;
extern uint64_t sessionid;
extern uint64_t query_id;
extern uint64_t slice_count;
extern uint64_t slice_seg_index;


extern bool kWaitingForNewQuery;
*/
extern bool kNotInitdb;
extern uint64_t kWorkerId;
extern uint64_t kCurrentIndex;
extern sdb::WorkerState kWorkerState;

namespace sdb {

struct CatalogInfo {
  // <catalog rel oid, version>
  std::unordered_map<uint64_t, std::string> catalog_version;
  std::mutex mtx_;
};

} // namespace sdb
