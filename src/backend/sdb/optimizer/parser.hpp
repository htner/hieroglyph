
#pragma once

#include "backend/sdb/common/pg_export.hpp"
#include "utils/palloc.h"
#include <butil/logging.h>

namespace sdb {

class Parser {
public:
	Parser() { 
    MemInit();
  }

  ~Parser() {
    if (parse_context_ != NULL) {
      MemoryContextDelete(parse_context_);
    }
  }

	void MemInit() {
		parse_context_ = AllocSetContextCreate(TopMemoryContext,
										                      "ParseContext",
										                      ALLOCSET_DEFAULT_SIZES);	
	}

	List* Parse(const char* query_string) {
		List* parsetree_list;


		auto oldcontext = MemoryContextSwitchTo(parse_context_);
		PG_TRY();
		{
			parsetree_list = raw_parser(query_string);
			elog_node_display(PG_LOG, "parse results:", parsetree_list, true);
		}
		PG_CATCH();
		{
			ErrorData *errdata;
			errdata = CopyErrorData();
			FlushErrorState();
			LOG(ERROR) << "pg analyze and rewrite failed: " << errdata->message;
			FreeErrorData(errdata);
			parsetree_list = nullptr;
		}
		PG_END_TRY();
    	MemoryContextSwitchTo(oldcontext);
		return parsetree_list;
	}

private:
  MemoryContext parse_context_ = NULL;
};

}
