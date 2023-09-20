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
	error_data_.set_severity(error_severity(edata->elevel));
	// error_data_.set_severity_unlocklized((edata->elevel));
    error_data_.set_code(code);

	if (edata->message) {
		error_data_.set_message(edata->message); 
	}
	if (edata->detail) {
		error_data_.set_detail(edata->detail); 
	}
    if (edata->hint) {
		error_data_.set_hint(edata->hint); 
	}
    if (edata->schema_name) {
		error_data_.set_schema_name(edata->schema_name); 
	}
    if (edata->table_name) {
		error_data_.set_table_name(edata->table_name); 
	}
    if (edata->column_name) {
		error_data_.set_column_name(edata->column_name); 
	}
  if (edata->datatype_name) {
		error_data_.set_data_type_name(edata->datatype_name); 
	}
    if (edata->constraint_name) {
		error_data_.set_constraint_name(edata->constraint_name); 
	}
  if (edata->filename) {
		error_data_.set_file(edata->filename); 
	}
	error_data_.set_line(edata->lineno); 

    if (edata->internalquery) {
		error_data_.set_internal_query(edata->internalquery); 
	}
	error_data_.set_internal_position(edata->internalpos); 
	error_data_.set_position(edata->cursorpos); 
}

}
