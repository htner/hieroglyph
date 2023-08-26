#pragma once

#include <memory>

#include <arrow/record_batch.h>
#include <arrow/array.h>

#include "backend/sdb/common/pg_export.hpp" 

namespace sdb
{

using ArrowToPgDatumConverter = std::function<arrow::Status(arrow::Array*, int64_t, Datum*)>;

class ArraySalarExchanger {
public:
  ArraySalarExchanger(Oid rel, Form_pg_attribute attr);
  ArraySalarExchanger(Oid rel, int16_t typid);

  ArrowToPgDatumConverter GetFunction(Oid rel, Form_pg_attribute attr);

  ArrowToPgDatumConverter GetFunction(Oid rel, Oid typid);

  ArrowToPgDatumConverter GetFunction(Oid rel,
                                      Oid typid,
                                      int typlen,
                                      bool typbyval,
                                      char typalign,
                                      char typtype, 
                                      int32_t typemod,
                                      Oid typelem,
                                      Oid typrelid);

  arrow::Status GetDatumByIndex(int64_t i, Datum* datum, bool* isnull);

  void SetArray(std::shared_ptr<arrow::Array> array) {
    array_ = array;
  }

  void SetName(const char* name) {
    name_ = name;
  }

  const std::string Name() {
    return name_;
  }

private:
  ArrowToPgDatumConverter func_;
  std::shared_ptr<arrow::Array> array_;
  Oid rel_;

  std::string name_;
};

}



