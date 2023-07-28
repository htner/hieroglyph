
#include <stdint.h>
#include <string>
//extern Oid MyDatabaseId;
//extern Oid MyDatabaseTableSpace;
//extern bool not_initdb;
//

std::string kDBBucket;
std::string kDBS3User;
std::string kDBS3Password;
std::string kDBS3Region;
std::string kDBS3Endpoint;
bool kDBIsMinio;

std::string kResultBucket;
std::string kResultS3User;
std::string kResultS3Password;
std::string kResultS3Region;
std::string kResultS3Endpoint;
bool kResultIsMinio;

uint64_t read_xid;
uint64_t commit_xid;
uint64_t dbid;
uint64_t sessionid;
uint64_t query_id;
uint64_t slice_count;
uint64_t slice_seg_index;





