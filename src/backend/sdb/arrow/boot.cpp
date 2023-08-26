#include "include/utils/fmgroids.h"
#include "include/catalog/pg_collation_d.h"
#include "backend/sdb/arrow/boot.hpp"
#include "backend/sdb/common/pg_export.hpp"

#include <butil/logging.h>

struct BootTypInfo {
	char		name[NAMEDATALEN];
	Oid			oid;
	Oid			elem;
	int16_t		len;
	bool		byval;
	char		align;
	char		storage;
	Oid			collation;
	Oid			inproc;
	Oid			outproc;
  int32_t typmod;
  Oid typrelid;
  char typtype;
} BootTypInfo;

static const struct BootTypInfo baseTypInfo[] = {
	{"bool", BOOLOID, 0, 1, true, 'c', 'p', InvalidOid,
	F_BOOLIN, F_BOOLOUT, -1, 0, 'b'},
	{"bytea", BYTEAOID, 0, -1, false, 'i', 'x', InvalidOid,
	F_BYTEAIN, F_BYTEAOUT, -1, 0, 'b'},
	{"char", CHAROID, 0, 1, true, 'c', 'p', InvalidOid,
	F_CHARIN, F_CHAROUT, -1, 0, 'b'},
	{"int2", INT2OID, 0, 2, true, 's', 'p', InvalidOid,
	F_INT2IN, F_INT2OUT, -1, 0, 'b'},
	{"int4", INT4OID, 0, 4, true, 'i', 'p', InvalidOid,
	F_INT4IN, F_INT4OUT, -1, 0, 'b'},
	{"float4", FLOAT4OID, 0, 4, FLOAT4PASSBYVAL, 'i', 'p', InvalidOid,
	F_FLOAT4IN, F_FLOAT4OUT, -1, 0, 'b'},
	{"name", NAMEOID, CHAROID, NAMEDATALEN, false, 'c', 'p', C_COLLATION_OID,
	F_NAMEIN, F_NAMEOUT, -1, 0, 'b'},
	{"regclass", REGCLASSOID, 0, 4, true, 'i', 'p', InvalidOid,
	F_REGCLASSIN, F_REGCLASSOUT, -1, 0, 'b'},
	{"regproc", REGPROCOID, 0, 4, true, 'i', 'p', InvalidOid,
	F_REGPROCIN, F_REGPROCOUT, -1, 0, 'b'},
	{"regtype", REGTYPEOID, 0, 4, true, 'i', 'p', InvalidOid,
	F_REGTYPEIN, F_REGTYPEOUT, -1, 0, 'b'},
	{"regrole", REGROLEOID, 0, 4, true, 'i', 'p', InvalidOid,
	F_REGROLEIN, F_REGROLEOUT, -1, 0, 'b'},
	{"regnamespace", REGNAMESPACEOID, 0, 4, true, 'i', 'p', InvalidOid,
	F_REGNAMESPACEIN, F_REGNAMESPACEOUT, -1, 0, 'b'},
	{"text", TEXTOID, 0, -1, false, 'i', 'x', DEFAULT_COLLATION_OID,
	F_TEXTIN, F_TEXTOUT, -1, 0, 'b'},
	{"oid", OIDOID, 0, 4, true, 'i', 'p', InvalidOid,
	F_OIDIN, F_OIDOUT, -1, 0, 'b'},
	{"tid", TIDOID, 0, 6, false, 's', 'p', InvalidOid,
	F_TIDIN, F_TIDOUT, -1, 0, 'b'},
	{"xid", XIDOID, 0, 4, true, 'i', 'p', InvalidOid,
	F_XIDIN, F_XIDOUT, -1, 0, 'b'},
	{"cid", CIDOID, 0, 4, true, 'i', 'p', InvalidOid,
	F_CIDIN, F_CIDOUT, -1, 'b'},
	{"pg_node_tree", PGNODETREEOID, 0, -1, false, 'i', 'x', DEFAULT_COLLATION_OID,
	F_PG_NODE_TREE_IN, F_PG_NODE_TREE_OUT, -1, 0, 'b'},
	{"int2vector", INT2VECTOROID, INT2OID, -1, false, 'i', 'p', InvalidOid,
	F_INT2VECTORIN, F_INT2VECTOROUT, -1, 0, 'b'},
	{"oidvector", OIDVECTOROID, OIDOID, -1, false, 'i', 'p', InvalidOid,
	F_OIDVECTORIN, F_OIDVECTOROUT, -1, 0, 'b'},
	{"_int4", INT4ARRAYOID, INT4OID, -1, false, 'i', 'x', InvalidOid,
	F_ARRAY_IN, F_ARRAY_OUT, -1, 0, 'b'},
	{"_text", 1009, TEXTOID, -1, false, 'i', 'x', DEFAULT_COLLATION_OID,
	F_ARRAY_IN, F_ARRAY_OUT, -1, 0, 'b'},
	{"_oid", 1028, OIDOID, -1, false, 'i', 'x', InvalidOid,
	F_ARRAY_IN, F_ARRAY_OUT, -1, 0, 'b'},
	{"_char", 1002, CHAROID, -1, false, 'i', 'x', InvalidOid,
	F_ARRAY_IN, F_ARRAY_OUT, -1, 0, 'b'},
	{ "aclitem", ACLITEMOID, 0, 12, false, 'i', 'p', InvalidOid, 
		1031, 1032, -1, 0, 'b'},
	{"_aclitem", 1034, ACLITEMOID, -1, false, 'i', 'x', InvalidOid,
	F_ARRAY_IN, F_ARRAY_OUT, -1, 0, 'b'}
};


static const int kTypes = sizeof(baseTypInfo) / sizeof(BootTypInfo);

bool GetBootTypeInfo(Oid typid, int32_t* typmod, 
						 char* typtype, int* typlen,
						 bool* byval, char* align,
						 Oid* elem, Oid* typrelid) {
	// LOG(ERROR) << "kTypes " << kTypes; 
	for (int i = 0; i < kTypes; ++i) {
		auto type = baseTypInfo[i];	
		if (type.oid == typid) {
			if (typmod) {
				*typmod = type.typmod;
			}
			if (typtype) {
				*typtype = type.typtype;
			}
			if (typlen) {
				*typlen = type.len;
			}
			if (byval) {
				*byval = type.byval;
			}
			if (align) {
				*align = type.align;
			}
			if (typrelid) {
				*typrelid = type.typrelid;
			}
			if (elem) {
				*elem = type.elem;
			}
			return true;
		}
	}
	return false;
}
