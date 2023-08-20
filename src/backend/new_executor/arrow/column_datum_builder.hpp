
namespace sdb {

class ColumnDatumBuildVisitor {
public:
  ColumnDatumBuildVisitor(FormData_pg_attribute attr) {
    attrtypid_ = attr->typid;
    attrtypmod_ = attr->atttypmod;

    HeapTuple tup;
    Form_gp_type elem_type;

    /* walk down to the base type */
    for (;;) {
      tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(atttypid));
      // elog(ERROR, "cache lookup failed for type: %u", atttypid);
      if (!HeapTupleIsValid(tup)) {
        return nullptr;
      }
      elem_type = (Form_pg_type) GETSTRUCT(tup);
      atttypid_ = elem_type->typbasetype;
      atttypmod_ = elem_type->typtypmod;
      typtype_ = elem_type->typtype;
      ReleaseSysCache(tup);
    }
    /* array type */
    if (typtype == 'c' && typelem != 0 && typlen == -1) {

      std::shared_ptr<DataType> elem_data_type = GetBaseDataType(typelem);
      if (elem_data_type != nullptr) {
        return arrow::list(elem_data_type);
      }
      return nullptr;

      MakeBuilder(pool, data_type, &array_builder_);

      if (elem_data_type != nullptr) {

        auto data_type = TypeMapping::GetDataType(attr);
        MakeBuilder(pool, data_type, &array_builder_);
      }
      return;
    }

    /* composite type */
    if (typrelid != 0) {
      Relation	relation;
      TupleDesc	tupdesc;
      std::vector<std::shared_ptr<Field>> sub_fields;

      relation = relation_open(typrelid, AccessShareLock);
      for (i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        auto sub_data_type = GetBaseDataType(attr->atttypid);
        if (sub_data_type == nullptr) {
          return nullptr;
        }
        sub_fields.emplace_back(NameStr(attr->attname), sub_data_type, !attr->attnonull);
      }
      return arrow::struct_(sub_fields);
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
      return dictionary(int32(), binary(), true);
    }
    auto data_type = TypeMapping::GetDataType(attr);
    MakeBuilder(pool, data_type, &array_builder_);
  }

private:
};

}
