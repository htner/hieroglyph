
#include "backend/new_executor/arrow/boot.hpp"
#include "backend/new_executor/arrow/column_builder.hpp"
#include "backend/new_executor/arrow/type_mapping.hpp"

#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <butil/logging.h>

//#include "backend/new_executor/arrow/column_builder.hpp"

#include <arrow/status.h>
#include <arrow/type_fwd.h>

#include "backend/new_executor/arrow/boot.hpp"



extern bool NeedForwardLookupFromPgType(Oid id);

extern bool NeedForwardLookupFromPgType(Oid id);

namespace pdb {

template <class TYPE_CLASS, class Value>
arrow::Status PutValue(arrow::ArrayBuilder* b, Value value, bool isnull) {
  using CType = typename arrow::TypeTraits<TYPE_CLASS>::CType;
  using BuilderType = typename arrow::TypeTraits<TYPE_CLASS>::BuilderType;

  auto builder = reinterpret_cast<BuilderType*>(b);
  if (isnull) {
    return builder->AppendNull();
  }
  return builder->Append(static_cast<CType>(value));
}

template <class TYPE_CLASS>
arrow::Status PutFixedString(arrow::ArrayBuilder* b, Datum d, size_t len,
                             bool isnull) {
  using BuilderType = typename arrow::TypeTraits<TYPE_CLASS>::BuilderType;

  auto builder = reinterpret_cast<BuilderType*>(b);
  if (isnull) {
    return builder->AppendNull();
  }
  std::string_view str(DatumGetPointer(d), len);

  return builder->Append(str);
}

PutDatumFunc GetPutFixedStringFunc(size_t len) {
  return std::bind(PutFixedString<arrow::FixedSizeBinaryType>, std::placeholders::_1,
                   std::placeholders::_2, len, std::placeholders::_3);
}

template <class TYPE_CLASS>
arrow::Status PutString(arrow::ArrayBuilder* b, Datum d, bool isnull) {
  using BuilderType = typename arrow::TypeTraits<TYPE_CLASS>::BuilderType;

  auto builder = reinterpret_cast<BuilderType*>(b);
  if (isnull) {
    return builder->AppendNull();
  }
  int vl_len = VARSIZE_ANY_EXHDR(d);
  char* vl_ptr = VARDATA_ANY(d);
  std::string_view str(vl_ptr, vl_len);

  return builder->Append(str);
}

/*
template<INT1OID>
arrow::Status PutDatum(arrow::ArrayBuilder* builder, Datum datum, bool isnull) {
        return PutValue<arrow::Int8Type>(builder, DatumGetInt8(datum), isnull);
}
*/

template <int>
arrow::Status PutDatum(arrow::ArrayBuilder* builder, Datum datum, bool isnull) {
  return arrow::Status::OK();
}

arrow::Status PutChar(arrow::ArrayBuilder* builder, Datum datum,
                                bool isnull) {
  return PutValue<arrow::UInt8Type>(builder, DatumGetChar(datum), isnull);
}

template <>
arrow::Status PutDatum<INT2OID>(arrow::ArrayBuilder* builder, Datum datum,
                                bool isnull) {
  return PutValue<arrow::Int16Type>(builder, DatumGetInt16(datum), isnull);
}

template <>
arrow::Status PutDatum<INT4OID>(arrow::ArrayBuilder* builder, Datum datum,
                                bool isnull) {
  return PutValue<arrow::Int32Type>(builder, DatumGetInt32(datum), isnull);
}

template <>
arrow::Status PutDatum<OIDOID>(arrow::ArrayBuilder* builder, Datum datum,
                               bool isnull) {
  return PutValue<arrow::UInt32Type>(builder, DatumGetUInt32(datum), isnull);
}

template <>
arrow::Status PutDatum<INT8OID>(arrow::ArrayBuilder* builder, Datum datum,
                                bool isnull) {
  return PutValue<arrow::Int64Type>(builder, DatumGetInt64(datum), isnull);
}

template <>
arrow::Status PutDatum<FLOAT4OID>(arrow::ArrayBuilder* builder, Datum datum,
                                  bool isnull) {
  return PutValue<arrow::FloatType>(builder, DatumGetFloat4(datum), isnull);
}

template <>
arrow::Status PutDatum<FLOAT8OID>(arrow::ArrayBuilder* builder, Datum datum,
                                  bool isnull) {
  return PutValue<arrow::DoubleType>(builder, DatumGetFloat8(datum), isnull);
}

template <>
arrow::Status PutDatum<BOOLOID>(arrow::ArrayBuilder* builder, Datum datum,
                                bool isnull) {
  return PutValue<arrow::BooleanType>(builder, DatumGetBool(datum), isnull);
}

template <>
arrow::Status PutDatum<BPCHAROID>(arrow::ArrayBuilder* builder, Datum datum,
                                  bool isnull) {
  return PutString<arrow::BinaryType>(builder, datum, isnull);
}

template <>
arrow::Status PutDatum<VARCHAROID>(arrow::ArrayBuilder* builder, Datum datum,
                                   bool isnull) {
  return PutString<arrow::BinaryType>(builder, datum, isnull);
}

template <>
arrow::Status PutDatum<BYTEAOID>(arrow::ArrayBuilder* builder, Datum datum,
                                 bool isnull) {
  return PutString<arrow::BinaryType>(builder, datum, isnull);
}

template <>
arrow::Status PutDatum<TEXTOID>(arrow::ArrayBuilder* builder, Datum datum,
                                bool isnull) {
  return PutString<arrow::BinaryType>(builder, datum, isnull);
}

template <>
arrow::Status PutDatum<TIMEOID>(arrow::ArrayBuilder* builder, Datum datum,
                                bool isnull) {
  return PutValue<arrow::Time64Type>(builder, DatumGetTimeADT(datum), isnull);
}

template <>
arrow::Status PutDatum<TIMESTAMPOID>(arrow::ArrayBuilder* builder, Datum datum,
                                     bool isnull) {
  return PutValue<arrow::Time64Type>(builder, DatumGetTimestamp(datum), isnull);
}

template <>
arrow::Status PutDatum<TIMESTAMPTZOID>(arrow::ArrayBuilder* builder,
                                       Datum datum, bool isnull) {
  return PutValue<arrow::Time64Type>(builder, DatumGetTimestampTz(datum),
                                     isnull);
}

template <>
arrow::Status PutDatum<DATEOID>(arrow::ArrayBuilder* builder, Datum datum,
                                bool isnull) {
  return PutValue<arrow::Time32Type>(builder, DatumGetDateADT(datum), isnull);
}

template <>
arrow::Status PutDatum<TIMETZOID>(arrow::ArrayBuilder* b, Datum datum,
                                  bool isnull) {
  using c_type = typename arrow::Time32Type::c_type;
  TimeTzADT* timetz = DatumGetTimeTzADTP(datum);
  auto builder = reinterpret_cast<arrow::Time32Builder*>(b);
  auto t = (timetz->time + (timetz->zone * 1000000.0));
  return builder->Append(static_cast<c_type>(t));
}

template <>
arrow::Status PutDatum<NUMERICOID>(arrow::ArrayBuilder* b, Datum datum,
                                   bool isnull) {
  char* tmp;
  tmp =
      DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(datum)));
  auto value = arrow::Decimal128::FromString(tmp);
  arrow::Decimal128 number = value.ValueOrDie();

  auto builder = reinterpret_cast<arrow::DecimalBuilder*>(b);
  //
  auto st = builder->Append(number);
  pfree(tmp);
  return st;
}

arrow::Status PutArray(PutDatumFunc sub_func, Oid sub_type, int sublen, 
						 bool elmbyval, char elmalign,
                        arrow::ArrayBuilder* b, Datum datum, bool isnull) {
  if (isnull) {
    return b->AppendNull();
  }

  Datum* datums;
  bool* nulls;
  int count;

  ArrayType* array = (ArrayType*)DatumGetPointer(datum);

  deconstruct_array(array, sub_type, sublen, elmbyval, elmalign, &datums, &nulls, &count);

  auto builder = reinterpret_cast<arrow::ListBuilder*>(b);
  auto status = builder->Append();
  if (!status.ok()) {
		return status;
  }
  arrow::ArrayBuilder* value_builder = builder->value_builder();

  for (int i = 0; i < count; ++i) {
    auto ret = sub_func(value_builder, datums[i], nulls[i]);
	assert(ret.ok());
	if (!ret.ok()) {
	  LOG(ERROR) << "put array function sub func error";
	  return ret;
	}
  }
  return arrow::Status::OK();
}

arrow::Status PutStruct(std::vector<PutDatumFunc> sub_funcs,
                        std::vector<Oid> sub_types, arrow::ArrayBuilder* b,
                        Datum d, bool isnull) {
  if (isnull) {
    return b->AppendNull();
  }

  int vl_len = VARSIZE_ANY_EXHDR(d);
  char* vl_ptr = VARDATA_ANY(d);
  std::string str(vl_ptr, vl_len);

  //auto builder = reinterpret_cast<arrow::StructBuilder*>(b);
  // return builder->Append(str);
  return arrow::Status::OK();
}

#define CASE_APPEND_TIMETZ(PG_TYPE, TYPE_CLASS, datum)               \
  case PG_TYPE: {                                                    \
    using c_type = typename TYPE_CLASS##Type::c_type;                \
    TimeTzADT* timetz = DatumGetTimeTzADTP;                          \
    auto builder = reinterpret_cast<TYPE_CLASS##Builder*>(builder_); \
    auto t = (timetz->time + (timetz->zone * 1000000.0));            \
    ARROW_RETURN_NOT_OK(builder->Append(static_cast<c_type>(t));		  \
    break;                                                           \
  }

ColumnBuilder::ColumnBuilder(Oid rel, Form_pg_attribute attr) {
  rel_ = rel;
  auto typid = attr->atttypid;
  auto typmod = attr->atttypmod;
  int typlen = attr->attlen;

  HeapTuple tup;
  Form_pg_type elem_type;
  char typtype;
  Oid attrelid;
  Oid attelem;

  if (!NeedForwardLookupFromPgType(rel)) {
	  bool ret = GetBootTypeInfo(typid, nullptr, &typtype, nullptr, 
							  nullptr, nullptr, &attelem, &attrelid);	
	  if (ret == false) {
		LOG(ERROR) << "get boot type error" << rel << " " << typid;
	   }
	  assert(ret);
  }

  /* walk down to the base type */
  while (NeedForwardLookupFromPgType(rel)) {
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tup)) {
      // TODO failure
      //elog(PANIC, "cache lookup failed for type: %u", typid);
      return;
    }
    elem_type = (Form_pg_type)GETSTRUCT(tup);
	if (typmod == -1) {
      typmod = elem_type->typtypmod;
	}

    typtype = elem_type->typtype;
    if (typtype != TYPTYPE_DOMAIN) {
	  typtype = elem_type->typtype;
	  attelem = elem_type->typelem;
	  attrelid = elem_type->typrelid;
      break;
    }

    typid = elem_type->typbasetype;
    ReleaseSysCache(tup);
  }
  put_value_func_ =
      GetPutValueFunction(typid, typlen, typtype, typmod,
                          attelem, attrelid);

  arrow_type_ = TypeMapping::GetDataType(attr);

  auto status =
      MakeBuilder(arrow::default_memory_pool(), arrow_type_, &array_builder_);
  assert(status.ok());
  assert(array_builder_ != nullptr);
}

#define CASE_RETURN_PUSH_VALUE_FUNC(oid) \
  case oid:                              \
    return PutDatum<oid>;

PutDatumFunc ColumnBuilder::GetPutValueFunction(Form_pg_attribute attr) {
  auto atttypid = attr->atttypid;
  auto atttypmod = attr->atttypmod;
  int typlen = attr->attlen;
  HeapTuple tup;
  Form_pg_type elem_type;

  char typtype = 'b';
  //auto attbyval = attr->attbyval;
  //auto attalign = attr->attalign;
  Oid attelem = 0;
  Oid  attrelid = 0;

  if (!NeedForwardLookupFromPgType(rel_)) {
	  bool ret = GetBootTypeInfo(atttypid, nullptr, &typtype, nullptr, 
							  nullptr, nullptr, &attelem, &attrelid);	
	  assert(ret);
  }
  /* walk down to the base type */
  while (NeedForwardLookupFromPgType(rel_)) {
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(atttypid));
    // elog(ERROR, "cache lookup failed for type: %u", atttypid);
    if (!HeapTupleIsValid(tup)) {
      return nullptr;
    }
    elem_type = (Form_pg_type)GETSTRUCT(tup);

    typtype = elem_type->typtype;

	if (atttypmod == -1) {
      atttypmod = elem_type->typtypmod;
	}

	if (typtype != TYPTYPE_DOMAIN) {
	  typtype = elem_type->typtype;
	  //attbyval = elem_type->typbyval;
	  //attalign = elem_type->typalign;
	  attelem = elem_type->typelem;
	  attrelid = elem_type->typrelid;
      break;
    }
    atttypid = elem_type->typbasetype;
    ReleaseSysCache(tup);
  }
  return GetPutValueFunction(atttypid, typlen, 
                     typtype, atttypmod,
                     attelem, attrelid);
}

void GetElmInfo(Oid rel, Oid typid, int32_t* typmod, 
						 char* typtype, int* typlen,
						 bool* elmbyval, char* elmalign) {

  HeapTuple tup;
  Form_pg_type elem_type;
  bool first = true;
  if (!NeedForwardLookupFromPgType(rel)) {
	  bool ret = GetBootTypeInfo(typid, typmod, typtype, typlen, 
							  elmbyval, elmalign, nullptr, nullptr);	
	  assert(ret);
  }

  /* walk down to the base type */
  while (NeedForwardLookupFromPgType(rel)) {
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    // elog(ERROR, "cache lookup failed for type: %u", atttypid);
    if (!HeapTupleIsValid(tup)) {
      return;
    }
    elem_type = (Form_pg_type)GETSTRUCT(tup);
    
	if (first) {
		*typlen = elem_type->typlen;
	}
	first = false;

	if (*typmod == -1) {
      *typmod = elem_type->typtypmod;
	}

    *typtype = elem_type->typtype;
	if (*typtype != TYPTYPE_DOMAIN) {
      *elmbyval = elem_type->typbyval;
      *elmalign = elem_type->typalign;
      break;
    }
    typid = elem_type->typbasetype;
    ReleaseSysCache(tup);
  }
}

PutDatumFunc ColumnBuilder::GetPutValueFunction(Oid typid) {
  HeapTuple tup;
  Form_pg_type elem_type;
  bool first = true;

  int32_t typmod = -1;
  char typtype;
  int typlen;  

  //bool attbyval;
  //char attalign;
  Oid attelem;
  Oid  attrelid;

  if (!NeedForwardLookupFromPgType(rel_)) {
	  bool ret = GetBootTypeInfo(typid, &typmod, &typtype, &typlen, 
							  nullptr, nullptr, &attelem, &attrelid);	
	  assert(ret);
  }
  /* walk down to the base type */
  while (NeedForwardLookupFromPgType(rel_)) {
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    // elog(ERROR, "cache lookup failed for type: %u", atttypid);
    if (!HeapTupleIsValid(tup)) {
      return nullptr;
    }
    elem_type = (Form_pg_type)GETSTRUCT(tup);
    
	if (first) {
		typlen = elem_type->typlen;
	}
	first = false;

    typtype = elem_type->typtype;

	if (typmod == -1) {
      typmod = elem_type->typtypmod;
	}
	if (typtype != TYPTYPE_DOMAIN) {
	  typtype = elem_type->typtype;
	  //attbyval = elem_type->typbyval;
	  //attalign = elem_type->typalign;
	  attelem = elem_type->typelem;
	  attrelid = elem_type->typrelid;
      break;
    }
    typid = elem_type->typbasetype;
    ReleaseSysCache(tup);
  }
  return GetPutValueFunction(typid, typlen, typtype,
                             typmod, attelem,
                             attrelid);
}

PutDatumFunc ColumnBuilder::GetPutValueFunction(Oid typid, int typlen,
                                                char typtype, int32_t typmod,
                                                Oid typelem, Oid typrelid) {
  /*
  HeapTuple tup;
  Form_gp_type elem_type;

  for (;;) {
          tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(atttypid));
                  // elog(ERROR, "cache lookup failed for type: %u", atttypid);
          if (!HeapTupleIsValid(tup)) {TYPTYPE_COMPOSITE
                  return nullptr;
          }
          elem_type = (Form_pg_type) GETSTRUCT(tup);
          atttypid_ = elem_type->typbasetype;
          atttypmod_ = elem_type->typtypmod;
          typtype_ = elem_type->typtype;
          ReleaseSysCache(tup);
  }
  */
  /* array type */
  if (typelem != 0 && typlen == -1) {
    auto sub_func = GetPutValueFunction(typelem);
    if (sub_func == nullptr) {
      return nullptr;
    }
	//assert(NeedForwardLookupFromPgType(rel_));
	int32_t typemod = -1; 
	char typtype;
	int attlen;
	bool elmbyval; 
	char elmalign;
	GetElmInfo(rel_, typelem, &typemod, 
						 &typtype, &attlen,
						 &elmbyval, &elmalign);
    return std::bind(PutArray, sub_func, typelem, attlen, elmbyval, elmalign, std::placeholders::_1,
                     std::placeholders::_2, std::placeholders::_3);
  }

  /* composite type */
  if (typtype == TYPTYPE_COMPOSITE && typrelid != 0) {
    Relation relation;
    std::vector<PutDatumFunc> sub_funcs;
    std::vector<Oid> sub_types;

    relation = relation_open(typrelid, AccessShareLock);
    TupleDesc tupdesc = RelationGetDescr(relation);
    for (int i = 0; i < tupdesc->natts; i++) {
      Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
      auto sub_typeid = attr->atttypid;
      auto sub_func = GetPutValueFunction(attr);
      sub_funcs.push_back(sub_func);
      sub_types.push_back(sub_typeid);
    }
    return std::bind(PutStruct, sub_funcs, sub_types, std::placeholders::_1,
                     std::placeholders::_2, std::placeholders::_3);
  }

  switch (typid) {
    CASE_RETURN_PUSH_VALUE_FUNC(INT2OID);
    CASE_RETURN_PUSH_VALUE_FUNC(INT4OID);
    CASE_RETURN_PUSH_VALUE_FUNC(OIDOID);
    CASE_RETURN_PUSH_VALUE_FUNC(INT8OID);
    CASE_RETURN_PUSH_VALUE_FUNC(FLOAT8OID);
    CASE_RETURN_PUSH_VALUE_FUNC(FLOAT4OID);
    CASE_RETURN_PUSH_VALUE_FUNC(BOOLOID);
    CASE_RETURN_PUSH_VALUE_FUNC(BPCHAROID);
    CASE_RETURN_PUSH_VALUE_FUNC(VARCHAROID);
    CASE_RETURN_PUSH_VALUE_FUNC(BYTEAOID);
    CASE_RETURN_PUSH_VALUE_FUNC(TEXTOID);
    CASE_RETURN_PUSH_VALUE_FUNC(TIMEOID);
    CASE_RETURN_PUSH_VALUE_FUNC(TIMESTAMPOID);
    CASE_RETURN_PUSH_VALUE_FUNC(TIMESTAMPTZOID);
    CASE_RETURN_PUSH_VALUE_FUNC(DATEOID);
    CASE_RETURN_PUSH_VALUE_FUNC(TIMETZOID);
    CASE_RETURN_PUSH_VALUE_FUNC(NUMERICOID);
    case NAMEOID:
      assert(typlen == NAMEDATALEN);
      // fallthrougth
    //case CHAROID:
      return GetPutFixedStringFunc(typlen);
    default: {
      /* elsewhere, we save the values just bunch of binary data */
      if (typlen > 0) {
        if (typlen == 1) {
          //return PutFixedString<arrow::FixedSizeBinaryType>;
		  return PutChar;
        } else if (typlen == 2) {
          return PutDatum<INT2OID>;
        } else if (typlen == 4) {
          return PutDatum<INT4OID>;
        } else if (typlen == 8) {
          return PutDatum<INT8OID>;
        }
        /*
         * MEMO: Unfortunately, we have no portable way to pack user defined
         * fixed-length binary data types, because their 'send' handler often
         * manipulate its internal data representation.
         * Please check box_send() for example. It sends four float8 (which
         * is reordered to bit-endien) values in 32bytes. We cannot understand
         * its binary format without proper knowledge.
         */
      } else if (typlen > 0) {
        return GetPutFixedStringFunc(typlen);
      } else if (typlen == -1) {
        return PutDatum<TEXTOID>;
      }
      // Elog("PostgreSQL type: '%s' is not supported", typname);
    }
  }
  return nullptr;
}

arrow::ArrayBuilder* ColumnBuilder::GetArrayBuilder() {
  return array_builder_.get();
}

}  // namespace pdb
