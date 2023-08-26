#include "backend/sdb/common/pg_export.hpp"
#include <stdint.h>

extern bool GetBootTypeInfo(Oid typid, int32_t* typmod, 
						 char* typtype, int* typlen,
						 bool* byval, char* align, Oid* elem, Oid* typrelid);
