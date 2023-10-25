#include "backend/sdb/common/pg_export.hpp"
extern "C" {
#include "include/sdb/session_info.h"
}
#include "backend/sdb/common/log.hpp"

void SendMessageToSession(ErrorData* edata) {
	if (thr_sess == nullptr) {
		return;
	}	
	if (thr_sess->log_context_ == nullptr) {
		thr_sess->log_context_ = (void*)NewObject(TopMemoryContext)sdb::LogDetail();
	}
    auto log_cxt = static_cast<sdb::LogDetail*>(thr_sess->log_context_);
	log_cxt->SendMessage(edata);
}

namespace sdb {

void sdb::LogDetail::SendMessage(ErrorData* edata) {
	char code[6];
	int ssval = edata->sqlerrcode;
	for (int i = 0; i < 5; i++)
	{
		code[i] = PGUNSIXBIT(ssval);
		ssval >>= 6;
	}
	code[5] = '\0';

	// TODO_SDB: more
    error_data_.set_message(edata->message); 
    error_data_.set_code(code);
	error_data_.set_severity(error_severity(edata->elevel));
}

}
