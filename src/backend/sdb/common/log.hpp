#pragma once
#include <string>
#include "backend/sdb/common/base_object.hpp"
#include "query.pb.h"

namespace sdb {

/*
struct PGErrorData {
  std::string sqlerrcode_;
  std::string message_;
};
*/

class LogDetail : public BaseObject {
public:
	void SendMessage(ErrorData* data);
	const sdb::ErrorResponse& LastErrorData() {
		return error_data_;
	}

private:
  sdb::ErrorResponse error_data_;
};

}
