
#pragma once

#include "backend/sdb/common/pg_export.hpp"

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
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(parse_context_);
		parsetree_list = raw_parser(query_string);
		return parsetree_list;
	}

private:
  MemoryContext parse_context_ = NULL;
};

}
