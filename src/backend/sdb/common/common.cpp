
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
bool kDBIsMinio = true;

std::string kResultBucket;
std::string kResultS3User;
std::string kResultS3Password;
std::string kResultS3Region;
std::string kResultS3Endpoint;
bool kResultIsMinio = true;

uint64_t read_xid = 0;
uint64_t commit_xid = 0;
uint64_t dbid = 0;
uint64_t sessionid = 1;
uint64_t query_id = 0;
uint64_t slice_count = 0;
uint64_t slice_seg_index = 0;

bool not_initdb = false;




