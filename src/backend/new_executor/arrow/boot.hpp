#include "backend/new_executor/pg.hpp"
#include <stdint.h>

extern bool GetBootTypeInfo(Oid typid, int32_t* typmod, 
						 char* typtype, int* typlen,
						 bool* byval, char* align, Oid* elem, Oid* typrelid);
