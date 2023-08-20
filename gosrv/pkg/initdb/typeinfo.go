package main

import "github.com/htner/sdb/gosrv/pkg/types"

// PostgreSQL oids for common types
const (
	InvalidOid = 0
	BOOLOID    = 16
	BYTEAOID   = 17
	//CHAROID               = 18
	CHAROID               = 18
	NAMEOID               = 19
	INT8OID               = 20
	INT2OID               = 21
	INT2VECTOROID         = 22
	INT4OID               = 23
	REGPROCOID            = 24
	TEXTOID               = 25
	OIDOID                = 26
	TIDOID                = 27
	XIDOID                = 28
	CIDOID                = 29
	OIDVECTOROID          = 30
	DEFAULT_COLLATION_OID = 100

	JSONOID             = 114
	JSONARRAYOID        = 199
	PGNODETREEOID       = 194
	POINTOID            = 600
	LSEGOID             = 601
	PATHOID             = 602
	BOXOID              = 603
	POLYGONOID          = 604
	LINEOID             = 628
	LINEARRAYOID        = 629
	CIDROID             = 650
	CIDRARRAYOID        = 651
	FLOAT4OID           = 700
	FLOAT8OID           = 701
	CIRCLEOID           = 718
	CIRCLEARRAYOID      = 719
	UNKNOWNOID          = 705
	MACADDROID          = 829
	INETOID             = 869
	C_COLLATION_OID     = 950
	BOOLARRAYOID        = 1000
	QCHARARRAYOID       = 1003
	NAMEARRAYOID        = 1003
	INT2ARRAYOID        = 1005
	INT4ARRAYOID        = 1007
	TEXTARRAYOID        = 1009
	TIDARRAYOID         = 1010
	BYTEAARRAYOID       = 1001
	XIDARRAYOID         = 1011
	CIDARRAYOID         = 1012
	BPCHARARRAYOID      = 1014
	VARCHARARRAYOID     = 1015
	INT8ARRAYOID        = 1016
	POINTARRAYOID       = 1017
	LSEGARRAYOID        = 1018
	PATHARRAYOID        = 1019
	BOXARRAYOID         = 1020
	FLOAT4ARRAYOID      = 1021
	FLOAT8ARRAYOID      = 1022
	POLYGONARRAYOID     = 1027
	OIDARRAYOID         = 1028
	ACLITEMOID          = 1033
	ACLITEMARRAYOID     = 1034
	MACADDRARRAYOID     = 1040
	INETARRAYOID        = 1041
	BPCHAROID           = 1042
	VARCHAROID          = 1043
	DATEOID             = 1082
	TIMEOID             = 1083
	TIMESTAMPOID        = 1114
	TIMESTAMPARRAYOID   = 1115
	DATEARRAYOID        = 1182
	TIMEARRAYOID        = 1183
	TIMESTAMPTZOID      = 1184
	TIMESTAMPTZARRAYOID = 1185
	INTERVALOID         = 1186
	INTERVALARRAYOID    = 1187
	NUMERICARRAYOID     = 1231
	BITOID              = 1560
	BITARRAYOID         = 1561
	VARBITOID           = 1562
	VARBITARRAYOID      = 1563
	NUMERICOID          = 1700

	REGCLASSOID       = 2205
	REGTYPEOID        = 2206
	RECORDOID         = 2249
	RECORDARRAYOID    = 2287
	UUIDOID           = 2950
	UUIDARRAYOID      = 2951
	JSONBOID          = 3802
	JSONBARRAYOID     = 3807
	DATERANGEOID      = 3912
	DATERANGEARRAYOID = 3913
	INT4RANGEOID      = 3904
	INT4RANGEARRAYOID = 3905
	NUMRANGEOID       = 3906
	NUMRANGEARRAYOID  = 3907
	TSRANGEOID        = 3908
	TSRANGEARRAYOID   = 3909
	TSTZRANGEOID      = 3910
	TSTZRANGEARRAYOID = 3911
	INT8RANGEOID      = 3926
	INT8RANGEARRAYOID = 3927
	REGNAMESPACEOID   = 4089
	REGROLEOID        = 4096

	INT4MULTIRANGEOID      = 4451
	NUMMULTIRANGEOID       = 4532
	TSMULTIRANGEOID        = 4533
	TSTZMULTIRANGEOID      = 4534
	DATEMULTIRANGEOID      = 4535
	INT8MULTIRANGEOID      = 4536
	INT4MULTIRANGEARRAYOID = 6150
	NUMMULTIRANGEARRAYOID  = 6151
	TSMULTIRANGEARRAYOID   = 6152
	TSTZMULTIRANGEARRAYOID = 6153
	DATEMULTIRANGEARRAYOID = 6155
	INT8MULTIRANGEARRAYOID = 6157
)
const (
	F_BYTEAOUT         = 31
	F_CHAROUT          = 33
	F_NAMEIN           = 34
	F_NAMEOUT          = 35
	F_INT2IN           = 38
	F_INT2OUT          = 39
	F_INT2VECTORIN     = 40
	F_INT2VECTOROUT    = 41
	F_INT4IN           = 42
	F_INT4OUT          = 43
	F_REGPROCIN        = 44
	F_REGPROCOUT       = 45
	F_TEXTIN           = 46
	F_TEXTOUT          = 47
	F_TIDIN            = 48
	F_TIDOUT           = 49
	F_XIDIN            = 50
	F_XIDOUT           = 51
	F_CIDIN            = 52
	F_CIDOUT           = 53
	F_OIDVECTORIN      = 54
	F_OIDVECTOROUT     = 55
	F_PG_NODE_TREE_IN  = 195
	F_PG_NODE_TREE_OUT = 196
	F_FLOAT4IN         = 200
	F_FLOAT4OUT        = 201
	F_ARRAY_IN         = 750
	F_ARRAY_OUT        = 751

	F_BOOLIN  = 1242
	F_BOOLOUT = 1243
	F_BYTEAIN = 1244
	F_CHARIN  = 1245

	F_OIDIN  = 1798
	F_OIDOUT = 1799

	F_REGPROCEDUREIN  = 2212
	F_REGPROCEDUREOUT = 2213
	F_REGOPERIN       = 2214
	F_REGOPEROUT      = 2215
	F_REGOPERATORIN   = 2216
	F_REGOPERATOROUT  = 2217
	F_REGCLASSIN      = 2218
	F_REGCLASSOUT     = 2219
	F_REGTYPEIN       = 2220
	F_REGTYPEOUT      = 2221

	F_REGNAMESPACEIN  = 4084
	F_REGNAMESPACEOUT = 4085
	F_REGROLEOUT      = 4092
	F_TO_REGROLE      = 4093
	F_REGROLERECV     = 4094
	F_REGROLESEND     = 4095
	F_REGROLEIN       = 4098
)

const (
	FLOAT4PASSBYVAL = true
	NAMEDATALEN     = 64
)

type TypeInfo struct {
	Name      string
	Oid       types.OID
	Elem      types.OID
	Len       int64
	Byval     bool
	Align     byte
	Storage   byte
	Collation types.OID
	Inproc    types.OID
	Outproc   types.OID
}

type TypeInfos struct {
	info []*TypeInfo
}

func (T *TypeInfos) add(t *TypeInfo) {
	T.info = append(T.info, t)
}

var InitTypeInfo TypeInfos

func init() {
	InitTypeInfo.add(&TypeInfo{"bool", BOOLOID, 0, 1, true, 'c', 'p', InvalidOid,
		F_BOOLIN, F_BOOLOUT})
	InitTypeInfo.add(&TypeInfo{"bytea", BYTEAOID, 0, -1, false, 'i', 'x', InvalidOid,
		F_BYTEAIN, F_BYTEAOUT})
	InitTypeInfo.add(&TypeInfo{"char", CHAROID, 0, 1, true, 'c', 'p', InvalidOid,
		F_CHARIN, F_CHAROUT})
	InitTypeInfo.add(&TypeInfo{"int2", INT2OID, 0, 2, true, 's', 'p', InvalidOid,
		F_INT2IN, F_INT2OUT})
	InitTypeInfo.add(&TypeInfo{"int4", INT4OID, 0, 4, true, 'i', 'p', InvalidOid,
		F_INT4IN, F_INT4OUT})
	InitTypeInfo.add(&TypeInfo{"float4", FLOAT4OID, 0, 4, FLOAT4PASSBYVAL, 'i', 'p', InvalidOid,
		F_FLOAT4IN, F_FLOAT4OUT})
	InitTypeInfo.add(&TypeInfo{"name", NAMEOID, CHAROID, NAMEDATALEN, false, 'c', 'p', C_COLLATION_OID,
		F_NAMEIN, F_NAMEOUT})
	InitTypeInfo.add(&TypeInfo{"regclass", REGCLASSOID, 0, 4, true, 'i', 'p', InvalidOid,
		F_REGCLASSIN, F_REGCLASSOUT})
	InitTypeInfo.add(&TypeInfo{"regproc", REGPROCOID, 0, 4, true, 'i', 'p', InvalidOid,
		F_REGPROCIN, F_REGPROCOUT})
	InitTypeInfo.add(&TypeInfo{"regtype", REGTYPEOID, 0, 4, true, 'i', 'p', InvalidOid,
		F_REGTYPEIN, F_REGTYPEOUT})
	InitTypeInfo.add(&TypeInfo{"regrole", REGROLEOID, 0, 4, true, 'i', 'p', InvalidOid,
		F_REGROLEIN, F_REGROLEOUT})
	InitTypeInfo.add(&TypeInfo{"regnamespace", REGNAMESPACEOID, 0, 4, true, 'i', 'p', InvalidOid,
		F_REGNAMESPACEIN, F_REGNAMESPACEOUT})
	InitTypeInfo.add(&TypeInfo{"text", TEXTOID, 0, -1, false, 'i', 'x', DEFAULT_COLLATION_OID,
		F_TEXTIN, F_TEXTOUT})
	InitTypeInfo.add(&TypeInfo{"oid", OIDOID, 0, 4, true, 'i', 'p', InvalidOid,
		F_OIDIN, F_OIDOUT})
	InitTypeInfo.add(&TypeInfo{"tid", TIDOID, 0, 6, false, 's', 'p', InvalidOid,
		F_TIDIN, F_TIDOUT})
	InitTypeInfo.add(&TypeInfo{"xid", XIDOID, 0, 4, true, 'i', 'p', InvalidOid,
		F_XIDIN, F_XIDOUT})
	InitTypeInfo.add(&TypeInfo{"cid", CIDOID, 0, 4, true, 'i', 'p', InvalidOid,
		F_CIDIN, F_CIDOUT})
	InitTypeInfo.add(&TypeInfo{"pg_node_tree", PGNODETREEOID, 0, -1, false, 'i', 'x', DEFAULT_COLLATION_OID,
		F_PG_NODE_TREE_IN, F_PG_NODE_TREE_OUT})
	InitTypeInfo.add(&TypeInfo{"int2vector", INT2VECTOROID, INT2OID, -1, false, 'i', 'p', InvalidOid,
		F_INT2VECTORIN, F_INT2VECTOROUT})
	InitTypeInfo.add(&TypeInfo{"oidvector", OIDVECTOROID, OIDOID, -1, false, 'i', 'p', InvalidOid,
		F_OIDVECTORIN, F_OIDVECTOROUT})
	InitTypeInfo.add(&TypeInfo{"_int4", INT4ARRAYOID, INT4OID, -1, false, 'i', 'x', InvalidOid,
		F_ARRAY_IN, F_ARRAY_OUT})
	InitTypeInfo.add(&TypeInfo{"_text", 1009, TEXTOID, -1, false, 'i', 'x', DEFAULT_COLLATION_OID,
		F_ARRAY_IN, F_ARRAY_OUT})
	InitTypeInfo.add(&TypeInfo{"_oid", 1028, OIDOID, -1, false, 'i', 'x', InvalidOid,
		F_ARRAY_IN, F_ARRAY_OUT})
	InitTypeInfo.add(&TypeInfo{"_char", 1002, CHAROID, -1, false, 'i', 'x', InvalidOid,
		F_ARRAY_IN, F_ARRAY_OUT})
	InitTypeInfo.add(&TypeInfo{"_aclitem", 1034, ACLITEMOID, -1, false, 'i', 'x', InvalidOid,
		F_ARRAY_IN, F_ARRAY_OUT})
}
