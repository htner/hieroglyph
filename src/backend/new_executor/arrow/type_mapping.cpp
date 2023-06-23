#include <assert.h>
#include "backend/new_executor/arrow/type_mapping.hpp"

extern "C" {
#include "access/relation.h"
}

namespace pdb {

// bool -> bool
// numeric -> Decimal
// bytea -> Binary
// date -> Date unit=Day
// time -> Time unit=MicroSecond
// interval -> Interval
// timestamp -> Timestamp unit=MicroSecond
// array -> List
// Struct -> compatible composite
// char(n) -> FixedSizeBinary(byteWidth)
//
std::shared_ptr<arrow::DataType> TypeMapping::GetBaseDataType(Oid typid,
                                                              int32_t typlen,
                                                              int32_t typmod) {
  const char* tzn = nullptr;
  int scale = 8;      /* default, if typmod == -1 */
  int precision = 30; /* default, if typmod == -1 */

  switch (typid) {
    case INT2OID:
      return arrow::int16();
    case INT4OID:
      return arrow::int32();
    case INT8OID:
      return arrow::int64();
    case FLOAT4OID:
      return arrow::float32();
    case FLOAT8OID:
      return arrow::float64();
    case BOOLOID:
      return arrow::boolean();
    case BPCHAROID:
    case VARCHAROID:
    case TEXTOID:
    case BYTEAOID:
      return arrow::binary();
    case TIMEOID:
      return arrow::time32(arrow::TimeUnit::MICRO);
    case TIMETZOID:
      // FIXME
      // tzn = show_timezone();
      // return arrow::time32(arrow::TimeUnit::MICRO, tzn);
      return arrow::time32(arrow::TimeUnit::MICRO);
    case TIMESTAMPOID:
      return arrow::timestamp(arrow::TimeUnit::MICRO);
    case TIMESTAMPTZOID:
      tzn = show_timezone();
      return arrow::timestamp(arrow::TimeUnit::MICRO, tzn);
    case DATEOID:
      return arrow::date64();
    case NUMERICOID:
      if (typmod >= (int32)(VARHDRSZ)) {
        typmod -= VARHDRSZ;
        precision = (typmod >> 16) & 0xffff;
        scale = typmod & 0xffff;
      }
      return arrow::decimal(precision, scale);
    case OIDOID:
		return arrow::uint32();
	case NAMEOID:
      assert(typlen == NAMEDATALEN);
    // fallthrougth
      return arrow::fixed_size_binary(typlen);
    default: {
      /* elsewhere, we save the values just bunch of binary data */
      if (typlen > 0) {
        if (typlen == 1) {
          return arrow::uint8();
        } else if (typlen == 2) {
          return arrow::int16();
        } else if (typlen == 4) {
          return arrow::int32();
        } else if (typlen == 8) {
          return arrow::int64();
        } else {
          return arrow::fixed_size_binary(typlen);
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
        return arrow::binary();
      }
      // Elog("PostgreSQL type: '%s' is not supported", typname);
    }
  }
  // assert(false);
  return nullptr;
}

std::shared_ptr<arrow::DataType> TypeMapping::GetDataType(
    Form_pg_attribute attr) {
  Oid atttypid = attr->atttypid;
  auto typmod = attr->atttypmod;
  auto typlen = attr->attlen;

  HeapTuple tup;
  Form_pg_type elem_type;
  char typtype;

  /* walk down to the base type */
  for (;;) {
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(atttypid));
    if (!HeapTupleIsValid(tup)) {
      // elog(ERROR, "cache lookup failed for type: %u", atttypid);
      return nullptr;
    }
    elem_type = (Form_pg_type)GETSTRUCT(tup);

    typtype = elem_type->typtype;
    if (typtype != TYPTYPE_DOMAIN) {
      typmod = elem_type->typtypmod;
      break;
    }
    atttypid = elem_type->typbasetype;
	ReleaseSysCache(tup);
  }

  return GetDataType(atttypid, elem_type->typelem, elem_type->typrelid,
                     typlen, typmod, typtype);
}

std::shared_ptr<arrow::DataType> TypeMapping::GetDataType(Oid typid) {
  HeapTuple tup;
  Form_pg_type elem_type;
  int32_t typmod;
  char typtype;
  bool first = true;
  int typlen;  
  /* walk down to the base type */
  for (;;) {
    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if (!HeapTupleIsValid(tup)) {
      // elog(ERROR, "cache lookup failed for type: %u", atttypid);
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

  return GetDataType(typid, elem_type->typelem, elem_type->typrelid,
                       typlen, typmod, typtype);
  return nullptr;
}

std::shared_ptr<arrow::DataType> TypeMapping::GetDataType(
    Oid typid, Oid typelem, Oid typrelid, int32_t typlen, int32_t typmod,
    char typtype) {
  /* array type */
  if (typelem != 0 && typlen == -1) {
    std::shared_ptr<arrow::DataType> elem_data_type = GetDataType(typelem);
    if (elem_data_type != nullptr) {
	  elog(WARNING, "sub type %s %c", elem_data_type->name(), typtype);
      return arrow::list(elem_data_type);
    }
	  elog(WARNING, "sub type not found %d", typelem);
    return nullptr;
  }

  /* composite type */
  if (typrelid != 0) {
    Relation relation;
    TupleDesc tupdesc;
    std::vector<std::shared_ptr<arrow::Field>> sub_fields;

    relation = relation_open(typrelid, AccessShareLock);
    tupdesc = RelationGetDescr(relation);
    for (int i = 0; i < tupdesc->natts; i++) {
      Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
      auto sub_data_type = GetDataType(attr->atttypid);
      if (sub_data_type == nullptr) {
        return nullptr;
      }
      std::string name = NameStr(attr->attname);
      sub_fields.push_back(std::make_shared<arrow::Field>(name, sub_data_type,
                                                          !attr->attnotnull));
    }
    return struct_(sub_fields);
  }

  /* enum type */
  if (typtype == 'c') {
    /* Scan pg_enum for the members of the target enum type. */
    /*
    ScanKeyInit(&skey,
              Anum_pg_enum_enumtypid,
              BTEqualStrategyNumber, F_OIDEQ,
              ObjectIdGetDatum(type_id));

    enum_rel = table_open(EnumRelationId, AccessShareLock);
    enum_scan = systable_beginscan(enum_rel,
                                                     EnumTypIdLabelIndexId,
                                                     true, NULL,
                                                     1, &skey);

    while (HeapTupleIsValid(enum_tuple = systable_getnext(enum_scan)))
    {
            Form_pg_enum en = (Form_pg_enum) GETSTRUCT(enum_tuple);
            en->enumsortorder;
            NameStr(en->enumlaber);
    }
    */
    return arrow::dictionary(arrow::int32(), arrow::binary(), true);
  }
  return GetBaseDataType(typid, typlen, typmod);
}

}  // namespace pdb
