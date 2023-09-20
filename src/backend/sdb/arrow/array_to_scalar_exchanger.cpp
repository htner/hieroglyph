#include "backend/sdb/arrow/array_to_scalar_exchanger.hpp"
#include "backend/sdb/arrow/boot.hpp"

extern "C" {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wregister"
#include "access/relation.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_database.h"
#include "utils/typcache.h"
#include "funcapi.h"
#pragma GCC diagnostic pop
}

#include "backend/sdb/common/pg_export.hpp"
#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include "backend/sdb/arrow/boot.hpp"

bool NeedForwardLookupFromPgType(Oid id) {
	if (id == TypeRelationId || id == ProcedureRelationId ||
	 id == DatabaseRelationId || id == RelationRelationId )
		return false;
	return true;
}

namespace sdb {

template <class TYPE_CLASS>
auto GetValue(const arrow::Array* a, int64_t i) {
  using ArrayType = typename arrow::TypeTraits<TYPE_CLASS>::ArrayType;

  auto array = reinterpret_cast<const ArrayType*>(a);
  return array->Value(i);
}

template <class TYPE_CLASS>
Datum GetStringToDatum(const arrow::Array* a, int64_t i) {
  using ArrayType = typename arrow::TypeTraits<TYPE_CLASS>::ArrayType;
  auto array = reinterpret_cast<const ArrayType*>(a);
  auto view = array->GetView(i);

  char* resultptr = (char*)palloc(view.size() + VARHDRSZ);
  SET_VARSIZE(resultptr, view.size() + VARHDRSZ);
  memcpy(VARDATA(resultptr), view.data(), view.size());
  return PointerGetDatum(resultptr);
}

template <class TYPE_CLASS>
arrow::Status GetFixStringToDatum(const arrow::Array* a,
								  size_t len, int64_t i, Datum* datum) {
  //LOG(ERROR) << "1 get name data  ";
  using ArrayType = typename arrow::TypeTraits<TYPE_CLASS>::ArrayType;
  auto array = reinterpret_cast<const ArrayType*>(a);
  auto view = array->GetView(i);
 
  char* resultptr = (char*)palloc(len);
  //LOG(ERROR) << "2 get name data  " << view;
  memcpy(resultptr, view.data(), view.size());
  *datum = PointerGetDatum(resultptr);
  return arrow::Status::OK();
}

ArrowToPgDatumConverter GetGetFixStringToDatumFunc(size_t len) {
  return std::bind(GetFixStringToDatum<arrow::FixedSizeBinaryType>, std::placeholders::_1,
                   len, std::placeholders::_2, std::placeholders::_3);
}

template <int>
arrow::Status GetDatum(const arrow::Array* array, int64_t i, Datum* datum) {
  return arrow::Status::OK();
}

arrow::Status GetChar(const arrow::Array* array, int64_t i, Datum* datum) {
  auto value = GetValue<arrow::UInt8Type>(array, i);
  *datum = CharGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<INT2OID>(const arrow::Array* array,
								int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Int16Type>(array, i);
  *datum = Int16GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<INT4OID>(const arrow::Array* array,
								int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Int32Type>(array, i);
  *datum = Int32GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<OIDOID>(const arrow::Array* array,
							   int64_t i, Datum* datum) {
  //LOG(ERROR) << " get datum oidoid 1";
  auto value = GetValue<arrow::UInt32Type>(array, i);
  //LOG(ERROR) << " get datum oidoid 2";
  *datum = UInt32GetDatum(value);
  //if (value == 2662)
  //	LOG(ERROR) << " get datum ["<< kRow__ << "]["<< i << "] value:" << value;
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<INT8OID>(const arrow::Array* array,
								int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Int64Type>(array, i);
  *datum = Int64GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<FLOAT4OID>(const arrow::Array* array,
								  int64_t i, Datum* datum) {
  auto value = GetValue<arrow::FloatType>(array, i);
  *datum = Float4GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<FLOAT8OID>(const arrow::Array* array,
								  int64_t i, Datum* datum) {
  auto value = GetValue<arrow::DoubleType>(array, i);
  *datum = Float8GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<BOOLOID>(const arrow::Array* array,
								int64_t i, Datum* datum) {
  auto value = GetValue<arrow::BooleanType>(array, i);
  *datum = BoolGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<BPCHAROID>(const arrow::Array* array,
								  int64_t i, Datum* datum) {
  *datum = GetStringToDatum<arrow::BinaryType>(array, i);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<VARCHAROID>(const arrow::Array* array,
								   int64_t i, Datum* datum) {
  *datum = GetStringToDatum<arrow::BinaryType>(array, i);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<BYTEAOID>(const arrow::Array* array,
								 int64_t i, Datum* datum) {
  *datum = GetStringToDatum<arrow::BinaryType>(array, i);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<TEXTOID>(const arrow::Array* array,
								int64_t i, Datum* datum) {
  *datum = GetStringToDatum<arrow::BinaryType>(array, i);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<CSTRINGOID>(const arrow::Array* a,
								   int64_t i, Datum* datum) {
  using ArrayType = typename arrow::TypeTraits<arrow::FixedSizeBinaryType>::ArrayType;
  auto array = reinterpret_cast<const ArrayType*>(a);
  auto view = array->GetView(i);

  bool is_cstring = view[view.size() - 1] == 0;
  auto len = is_cstring ? view.size() : view.size() + 1; 
  char* resultptr = (char*)palloc0(len);
  memcpy(resultptr, view.data(), view.size());
  *datum = PointerGetDatum(resultptr);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<TIMEOID>(const arrow::Array* array,
								int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Time64Type>(array, i);
  *datum = TimeADTGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<TIMESTAMPOID>(const arrow::Array* array,
									 int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Time64Type>(array, i);
  *datum = TimeADTGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<TIMESTAMPTZOID>(const arrow::Array* array,
									   int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Time64Type>(array, i);
  *datum = TimeADTGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<DATEOID>(const arrow::Array* array,
								int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Date32Type>(array, i);
  *datum = DateADTGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<TIMETZOID>(const arrow::Array* array,
								  int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Time32Type>(array, i);
  *datum = DateADTGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<NUMERICOID>(const arrow::Array* a,
								   int64_t i, Datum* datum) {
  auto array = reinterpret_cast<const arrow::DecimalArray*>(a);
  auto value = array->FormatValue(i);
  auto value_datum = CStringGetDatum(value.data());

  *datum = DirectFunctionCall3(numeric_in, value_datum, ObjectIdGetDatum(0),
                               Int32GetDatum(-1));

  return arrow::Status::OK();
}

arrow::Status GetArray(ArrowToPgDatumConverter sub_func, Oid sub_type,
					   int32_t elmlen, bool elmbyval,
					   char elmalign, const arrow::Array* a,
					   int64_t i, Datum* datum) {
  Datum* datums;
  bool* nulls;

  const arrow::ListArray* list_array = reinterpret_cast<const arrow::ListArray*>(a);
  std::shared_ptr<arrow::Array> array = list_array->value_slice(i);

  auto size = array->length();
  if (size == 0) {
	datums = NULL;
	nulls = NULL;
  } else {
	datums = (Datum*)palloc(size * sizeof(Datum));
	nulls = (bool*)palloc(size * sizeof(bool));
  }
  //LOG(ERROR) << "get array size " << size; 
  for (int i = 0; i < size; ++i) {
    if (array->IsNull(i)) {
      nulls[i] = true;
      datums[i] = PointerGetDatum(NULL);
      continue;
    }
    nulls[i] = false;
    auto status = sub_func(array.get(), i, &(datums[i]));
	if (!status.ok()) {
		return status;
	}
  }

  ArrayType* t =
      construct_array(datums, size, sub_type, elmlen, elmbyval, elmalign);

  *datum = PointerGetDatum(t);
  if (datums) {
    pfree(datums);
    pfree(nulls);
  }
  return arrow::Status::OK();
}

arrow::Status GetStruct(std::vector<ArrowToPgDatumConverter> sub_funcs, 
						Oid typid, std::vector<Oid> sub_types, 
						const arrow::Array* array,
                        int64_t i, Datum* d) {
  if (array->IsNull(i)) {
	*d = PointerGetDatum(NULL);
  }
  Datum* datums;
  bool* nulls;

  auto size = sub_funcs.size();
  datums = (Datum*)palloc(size * sizeof(Datum));
  nulls = (bool*)palloc(size * sizeof(bool));

  auto typentry =
		lookup_type_cache(typid, (TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO));

  auto struct_array = reinterpret_cast<const arrow::StructArray*>(array);

  for (size_t j = 0; j < sub_funcs.size(); ++j) {
	auto field_array = struct_array->field(j);
	arrow::Status s = sub_funcs[i](field_array.get(), i, datums + j);
	if (!s.ok()) {
		return s;
	}
  }
  auto resultTuple = heap_form_tuple(typentry->tupDesc, datums, nulls);
  *d = HeapTupleGetDatum(resultTuple);
  pfree(datums);
  pfree(nulls);
  return arrow::Status::OK();
}

ArraySalarExchanger::ArraySalarExchanger(Oid rel, Form_pg_attribute attr) {
	//LOG(ERROR) << "get boot type " << rel << " " << attr->atttypid;
	func_ = GetFunction(rel, attr);
	if (func_ == nullptr) {
		LOG(ERROR) << "get boot type eror " << rel << " " << attr->atttypid;
	} else {
		//LOG(ERROR) << "get boot type finish" << rel << " " << attr->atttypid;
	}
}

ArraySalarExchanger::ArraySalarExchanger(Oid rel, int16_t typid) { 
	func_ = GetFunction(rel, typid);
}

#define CASE_RETURN_PUSH_VALUE_FUNC(oid) \
  case oid: {                              \
    return GetDatum<oid>; \
 }

ArrowToPgDatumConverter ArraySalarExchanger::GetFunction(Oid rel, Form_pg_attribute attr) {
  HeapTuple tup;
  Form_pg_type elem_type;
  auto atttypid = attr->atttypid;
  auto atttypmod = attr->atttypmod;
  auto typlen = attr->attlen;
  char typtype = 'b';
  auto attbyval = attr->attbyval;
  auto attalign = attr->attalign;
  Oid attelem = 0;
  Oid  attrelid = 0;
  rel_ = rel;

  if (!NeedForwardLookupFromPgType(rel)) {
	  bool ret = GetBootTypeInfo(atttypid, nullptr, &typtype, nullptr, 
							  nullptr, nullptr, &attelem, &attrelid);	
	  if (ret == false) {
		LOG(ERROR) << "get boot type error" << rel << " " << atttypid;
	   }
	  assert(ret);
  }
  
  /* walk down to the base type */
  while (NeedForwardLookupFromPgType(rel_)) {
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(atttypid));
    // elog(ERROR, "cache lookup failed for type: %u", atttypid);
    if (!HeapTupleIsValid(tup)) {
	  LOG(ERROR) << "return null, need crash " << atttypid; 
	  std::string* core = nullptr;
	  *core = "0";
      return nullptr;
    }
    elem_type = (Form_pg_type)GETSTRUCT(tup);

    typtype = elem_type->typtype;

	if (atttypmod == -1) {
      atttypmod = elem_type->typtypmod;
	}

	if (typtype != TYPTYPE_DOMAIN) {
	  typtype = elem_type->typtype;
	  attbyval = elem_type->typbyval;
	  attalign = elem_type->typalign;
	  attelem = elem_type->typelem;
	  attrelid = elem_type->typrelid;
      break;
    }
    atttypid = elem_type->typbasetype;
	ReleaseSysCache(tup);
  }

  return GetFunction(rel, atttypid, typlen, attbyval,
                     attalign, typtype, atttypmod,
                     attelem, attrelid);
}

ArrowToPgDatumConverter ArraySalarExchanger::GetFunction(Oid rel, Oid typid) {
  HeapTuple tup;
  Form_pg_type elem_type;

  char typtype;
  bool first = true;
  int typlen;  
  int32_t atttypmod = -1;
  bool attbyval = false;
  char attalign;
  Oid attelem = 0;
  Oid  attrelid = 0;

  if (!NeedForwardLookupFromPgType(rel_)) {
//	LOG(ERROR) << " 1 get boot type error" << rel_ << " " << typid; 
	bool ret = GetBootTypeInfo(typid, &atttypmod, &typtype, &typlen, 
							&attbyval, &attalign, &attelem, &attrelid);

	//LOG(ERROR) << " 2 get boot type error" << rel_ << " " << typid << " " << ret; 
	if (ret == false) {
		LOG(ERROR) << "3 get boot type error" << rel_ << " " << typid; 
	}
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
	if (atttypmod == -1) {
      atttypmod = elem_type->typtypmod;
	}

    typtype = elem_type->typtype;
    if (typtype != TYPTYPE_DOMAIN) {
	  typtype = elem_type->typtype;
	  attbyval = elem_type->typbyval;
	  attalign = elem_type->typalign;
	  attelem = elem_type->typelem;
	  attrelid = elem_type->typrelid;
      break;
    }

    typid = elem_type->typbasetype;
    ReleaseSysCache(tup);
  }
	/*
  return GetFunction(typid, elem_type->typlen, elem_type->typbyval,
                     elem_type->typalign, elem_type->typtype,
                     elem_type->typtypmod, elem_type->typelem,
                     elem_type->typrelid);
					 */
  return GetFunction(rel, typid, typlen, attbyval,
                     attalign, typtype, atttypmod,
                     attelem, attrelid);
}

extern void GetElmInfo(Oid rel, Oid typid, int32_t* typmod, 
						 char* typtype, int* typlen,
						 bool* elmbyval, char* elmalign);

ArrowToPgDatumConverter ArraySalarExchanger::GetFunction(Oid rel, Oid typid, int typlen, bool typbyval,
                                          char typalign, char typtype,
                                          int32_t typemod, Oid typelem,
                                          Oid typrelid) {
  /* array type */
  if (typelem != 0 && typlen == -1) {
	// LOG(ERROR) << "get array function";
    auto sub_func = GetFunction(rel, typelem);
    if (sub_func == nullptr) {
	
	  LOG(ERROR) << "get array function sub func error";
      return nullptr;
    }
    /*
    int32_t elmlen,
    bool elmbyval,
    char elmalign,
    */
	int32_t typemod; 
	char typtype;
	int attlen;
	bool elmbyval; 
	char elmalign;
	GetElmInfo(rel_, typelem, &typemod, 
						 &typtype, &attlen,
						 &elmbyval, &elmalign);
    
    return std::bind(GetArray, sub_func, typelem, attlen, elmbyval, elmalign,
                     std::placeholders::_1, std::placeholders::_2,
                     std::placeholders::_3);
  }

  /* composite type */
  if (typtype == TYPTYPE_COMPOSITE && typrelid != 0) {
    Relation relation;
	LOG(ERROR) << "get composite function";
    std::vector<ArrowToPgDatumConverter> sub_funcs;
    std::vector<Oid> sub_types;

    relation = relation_open(typrelid, AccessShareLock);
    TupleDesc tupdesc = RelationGetDescr(relation);
    for (int i = 0; i < tupdesc->natts; i++) {
      Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
      auto sub_typeid = attr->atttypid;
      auto sub_func = GetFunction(typrelid, attr);
      sub_funcs.push_back(sub_func);
      sub_types.push_back(sub_typeid);
    }
	relation_close(relation, AccessShareLock);
    return std::bind(GetStruct, sub_funcs, typid, sub_types, std::placeholders::_1,
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
	  // LOG(ERROR) << "get name function";
      assert(typlen == NAMEDATALEN);
	  return GetGetFixStringToDatumFunc(typlen); 
	default: {
      /* elsewhere, we save the values just bunch of binary data */
      if (typlen > 0) {
        if (typlen == 1) {
          return GetChar;
          // return GetStringToDatum<arrow::FixedSizeBinaryType>;
        } else if (typlen == 2) {
          return GetDatum<INT2OID>;
        } else if (typlen == 4) {
          return GetDatum<INT4OID>;
        } else if (typlen == 8) {
          return GetDatum<INT8OID>;
        } else {
	//	LOG(ERROR) << "try to get default function" << typlen;
          return GetGetFixStringToDatumFunc(typlen);
        }
        /*
         * MEMO: Unfortunately, we have no portable way to pack user defined
         * fixed-length binary data types, because their 'send' handler often
         * manipulate its internal data representation.
         * Please check box_send() for example. It sends four float8 (which
         * is reordered to bit-endien) values in 32bytes. We cannot understand
         * its binary format without proper knowledge.
         */
      } else if (typlen == -1) {
        return GetDatum<TEXTOID>;
      } else if (typlen == -2) {
        return GetDatum<CSTRINGOID>;
	  }
      // Elog("PostgreSQL type: '%s' is not supported", typname);
    }
  }
  LOG(ERROR) << "try to get nullptr function";
  return nullptr;
}

arrow::Status ArraySalarExchanger::GetDatumByIndex(int64_t i, Datum* datum,
                                               bool* isnull) {
  //LOG(ERROR) << " get datum by index 1";
  if (array_ == nullptr) {
    return arrow::Status::IndexError("array empty");
  }
  if (i >= array_->length()) {
    return arrow::Status::IndexError("out of index");
  }
  if (array_->IsNull(i)) {
    *isnull = true;
    return arrow::Status::OK();
  }
  *isnull = false;
  //LOG(ERROR) << " get datum by index 2, rel " << rel_ << " " << array_->length();
  auto ret = func_(array_.get(), i, datum);
  //LOG(ERROR) << " get datum by index 3";
  return ret;
}

}  // namespace pdb
