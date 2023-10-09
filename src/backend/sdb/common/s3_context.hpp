#pragma once
#include <string>
#include "backend/sdb/common/base_object.hpp"

struct S3Context : public BaseObject {
	std::string lake_bucket_;
	std::string lake_user_;
	std::string lake_password_;
	std::string lake_region_;
	std::string lake_endpoint_;
	bool lake_isminio_;

	std::string result_bucket_;
	std::string result_user_;
	std::string result_password_;
	std::string result_region_;
	std::string result_endpoint_;
	bool result_isminio_;
};

extern S3Context* GetS3Context();
