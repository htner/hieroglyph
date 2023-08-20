
#include <stdint.h>
#include <string>
//extern Oid MyDatabaseId;
//extern Oid MyDatabaseTableSpace;
//extern bool not_initdb;
//

std::string kDBBucket = "sdb1";
std::string kDBS3User = "minioadmin";
std::string kDBS3Password = "minioadmin";
std::string kDBS3Region = "ap1";
std::string kDBS3Endpoint = "127.0.0.1:9000";
bool kDBIsMinio = true;

std::string kResultBucket = "sdb1";
std::string kResultS3User = "minioadmin";
std::string kResultS3Password = "minioadmin";
std::string kResultS3Region = "ap1";
std::string kResultS3Endpoint = "127.0.0.1:9000";
bool kResultIsMinio = true;

uint64_t dbid = 1;
uint64_t sessionid = 1;
uint64_t query_id = 0;
uint64_t slice_count = 0;
uint64_t slice_seg_index = 0;

bool not_initdb = false;




