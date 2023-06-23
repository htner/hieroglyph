#include "backend/new_executor/arrow/column_exchanger.hpp"

extern "C" {
#include "access/relation.h"
}

namespace pdb {

template <class TYPE_CLASS>
// auto GetValue(arrow::Array* a, int64_t i) ->
// decltype(arrow::TypeTraits<TYPE_CLASS>::CType) { decltype(auto)
// GetValue(arrow::Array* a, int64_t i) { //}-> decltype(CType) {
auto GetValue(arrow::Array* a, int64_t i) {  //}-> decltype(CType) {
  // using CType = typename arrow::TypeTraits<TYPE_CLASS>::CType;
  using ArrayType = typename arrow::TypeTraits<TYPE_CLASS>::ArrayType;

  auto array = reinterpret_cast<ArrayType*>(a);
  return array->Value(i);
}

template <class TYPE_CLASS>
Datum GetStringToDatum(arrow::Array* a, int64_t i) {
  using ArrayType = typename arrow::TypeTraits<TYPE_CLASS>::ArrayType;
  auto array = reinterpret_cast<ArrayType*>(a);
  auto view = array->GetView(i);
  if (view.size() > 100000) {
    elog(PANIC, "");
  }

  char* resultptr = (char*)palloc(view.size() + VARHDRSZ);
  SET_VARSIZE(resultptr, view.size() + VARHDRSZ);
  memcpy(VARDATA(resultptr), view.data(), view.size());
  return PointerGetDatum(resultptr);
}

template <class TYPE_CLASS>
arrow::Status GetFixStringToDatum(arrow::Array* a, size_t len, int64_t i, Datum* datum) {
  using ArrayType = typename arrow::TypeTraits<TYPE_CLASS>::ArrayType;
  auto array = reinterpret_cast<ArrayType*>(a);
  auto view = array->GetView(i);
  if (view.size() > 100000) {
    elog(PANIC, "");
  }
  if (view.size() > len) {
    elog(PANIC, "");
  }

  char* resultptr = (char*)palloc(len);
  memcpy(resultptr, view.data(), view.size());
  *datum = PointerGetDatum(resultptr);
  return arrow::Status::OK();
}

GetDatumFunc GetGetFixStringToDatumFunc(size_t len) {
  return std::bind(GetFixStringToDatum<arrow::FixedSizeBinaryType>, std::placeholders::_1,
                   len, std::placeholders::_2, std::placeholders::_3);
}

template <int>
arrow::Status GetDatum(arrow::Array* array, int64_t i, Datum* datum) {
  return arrow::Status::OK();
}

arrow::Status GetChar(arrow::Array* array, int64_t i, Datum* datum) {
  auto value = GetValue<arrow::UInt8Type>(array, i);
  *datum = CharGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<INT2OID>(arrow::Array* array, int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Int16Type>(array, i);
  *datum = Int16GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<INT4OID>(arrow::Array* array, int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Int32Type>(array, i);
  *datum = Int32GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<OIDOID>(arrow::Array* array, int64_t i, Datum* datum) {
  auto value = GetValue<arrow::UInt32Type>(array, i);
  *datum = UInt32GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<INT8OID>(arrow::Array* array, int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Int64Type>(array, i);
  *datum = Int64GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<FLOAT4OID>(arrow::Array* array, int64_t i,
                                  Datum* datum) {
  auto value = GetValue<arrow::FloatType>(array, i);
  *datum = Float4GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<FLOAT8OID>(arrow::Array* array, int64_t i,
                                  Datum* datum) {
  auto value = GetValue<arrow::DoubleType>(array, i);
  *datum = Float8GetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<BOOLOID>(arrow::Array* array, int64_t i, Datum* datum) {
  auto value = GetValue<arrow::BooleanType>(array, i);
  *datum = BoolGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<BPCHAROID>(arrow::Array* array, int64_t i,
                                  Datum* datum) {
  *datum = GetStringToDatum<arrow::BinaryType>(array, i);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<VARCHAROID>(arrow::Array* array, int64_t i,
                                   Datum* datum) {
  *datum = GetStringToDatum<arrow::BinaryType>(array, i);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<BYTEAOID>(arrow::Array* array, int64_t i, Datum* datum) {
  *datum = GetStringToDatum<arrow::BinaryType>(array, i);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<TEXTOID>(arrow::Array* array, int64_t i, Datum* datum) {
  *datum = GetStringToDatum<arrow::BinaryType>(array, i);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<TIMEOID>(arrow::Array* array, int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Time64Type>(array, i);
  *datum = TimeADTGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<TIMESTAMPOID>(arrow::Array* array, int64_t i,
                                     Datum* datum) {
  auto value = GetValue<arrow::Time64Type>(array, i);
  *datum = TimeADTGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<TIMESTAMPTZOID>(arrow::Array* array, int64_t i,
                                       Datum* datum) {
  auto value = GetValue<arrow::Time64Type>(array, i);
  *datum = TimeADTGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<DATEOID>(arrow::Array* array, int64_t i, Datum* datum) {
  auto value = GetValue<arrow::Time32Type>(array, i);
  *datum = DateADTGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<TIMETZOID>(arrow::Array* array, int64_t i,
                                  Datum* datum) {
  auto value = GetValue<arrow::Time32Type>(array, i);
  *datum = DateADTGetDatum(value);
  return arrow::Status::OK();
}

template <>
arrow::Status GetDatum<NUMERICOID>(arrow::Array* a, int64_t i, Datum* datum) {
  auto array = reinterpret_cast<arrow::DecimalArray*>(a);
  auto value = array->FormatValue(i);
  auto value_datum = CStringGetDatum(value.data());

  *datum = DirectFunctionCall3(numeric_in, value_datum, ObjectIdGetDatum(0),
                               Int32GetDatum(-1));

  return arrow::Status::OK();
}

arrow::Status GetArray(GetDatumFunc sub_func, Oid sub_type, int32_t elmlen,
                       bool elmbyval, char elmalign, arrow::Array* a, int64_t i,
                       Datum* datum) {
  Datum* datums;
  bool* nulls;

  arrow::ListArray* list_array = reinterpret_cast<arrow::ListArray*>(a);
  std::shared_ptr<arrow::Array> array = list_array->value_slice(i);

  auto size = array->length();
  datums = (Datum*)palloc(size * sizeof(Datum));
  nulls = (bool*)palloc(size * sizeof(bool));
  for (int i = 0; i < size; ++size) {
    if (array->IsNull(i)) {
      nulls[i] = true;
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

  *datum = (Datum)t;
  pfree(datums);
  pfree(nulls);
  return arrow::Status::OK();
}

arrow::Status GetStruct(std::vector<GetDatumFunc> sub_funcs,
                        std::vector<Oid> sub_types, arrow::Array* array,
                        int64_t i, Datum* d) {
  return arrow::Status::OK();
}

ColumnExchanger::ColumnExchanger(Form_pg_attribute attr) {
  func_ = GetFunction(attr);
}

ColumnExchanger::ColumnExchanger(int16_t typid) { func_ = GetFunction(typid); }

#define CASE_RETURN_PUSH_VALUE_FUNC(oid) \
  case oid:                              \
    return GetDatum<oid>;

GetDatumFunc ColumnExchanger::GetFunction(Form_pg_attribute attr) {
  HeapTuple tup;
  Form_pg_type elem_type;
  auto atttypid = attr->atttypid;
  auto atttypmod = attr->atttypmod;
  auto typlen = attr->attlen;
  char typtype;
  /* walk down to the base type */
  for (;;) {
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(atttypid));
    // elog(ERROR, "cache lookup failed for type: %u", atttypid);
    if (!HeapTupleIsValid(tup)) {
      return nullptr;
    }
    elem_type = (Form_pg_type)GETSTRUCT(tup);

    typtype = elem_type->typtype;

	if (typtype != TYPTYPE_DOMAIN) {
      atttypmod = elem_type->typtypmod;
      break;
    }
    atttypid = elem_type->typbasetype;
	ReleaseSysCache(tup);
  }
  return GetFunction(atttypid, typlen, elem_type->typbyval,
                     elem_type->typalign, elem_type->typtype, atttypmod,
                     elem_type->typelem, elem_type->typrelid);
}

GetDatumFunc ColumnExchanger::GetFunction(Oid typid) {
  HeapTuple tup;
  Form_pg_type elem_type;

  int32_t typmod;
  char typtype;
  bool first = true;
  int typlen;  
  /* walk down to the base type */
  for (;;) {
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
    if (typtype != TYPTYPE_DOMAIN) {
      typmod = elem_type->typtypmod;
      break;
    }

    typid = elem_type->typbasetype;
    ReleaseSysCache(tup);
  }
  return GetFunction(typid, elem_type->typlen, elem_type->typbyval,
                     elem_type->typalign, elem_type->typtype,
                     elem_type->typtypmod, elem_type->typelem,
                     elem_type->typrelid);
}

GetDatumFunc ColumnExchanger::GetFunction(Oid typid, int typlen, bool typbyval,
                                          char typalign, char typtype,
                                          int32_t typemod, Oid typelem,
                                          Oid typrelid) {
  /* array type */
  if (typelem != 0 && typlen == -1) {
    auto sub_func = GetFunction(typelem);
    if (sub_func == nullptr) {
      return nullptr;
    }
    /*
    int32_t elmlen,
    bool elmbyval,
    char elmalign,
    */
    return std::bind(GetArray, sub_func, typelem, typlen, typbyval, typalign,
                     std::placeholders::_1, std::placeholders::_2,
                     std::placeholders::_3);
  }

  /* composite type */
  if (typtype == TYPTYPE_COMPOSITE && typrelid != 0) {
    // Relation relation;
    TupleDesc tupdesc;
    std::vector<GetDatumFunc> sub_funcs;
    std::vector<Oid> sub_types;

    // relation = relation_open(typrelid, AccessShareLock);
    for (int i = 0; i < tupdesc->natts; i++) {
      Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
      auto sub_data_type = GetFunction(attr);
      if (sub_data_type == nullptr) {
        return nullptr;
      }
      auto sub_typeid = attr->atttypid;
      auto sub_func = GetFunction(sub_typeid);
      sub_funcs.push_back(sub_func);
      sub_types.push_back(sub_typeid);
    }
    return std::bind(GetStruct, sub_funcs, sub_types, std::placeholders::_1,
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
      }
      // Elog("PostgreSQL type: '%s' is not supported", typname);
    }
  }
  return nullptr;
}

arrow::Status ColumnExchanger::GetDatumByIndex(int64_t i, Datum* datum,
                                               bool* isnull) {
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
  return func_(array_.get(), i, datum);
}

}  // namespace pdb
