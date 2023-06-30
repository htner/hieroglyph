#pragma once

#include <memory>

#include <arrow/record_batch.h>
#include <arrow/array.h>

#include "backend/new_executor/pg.hpp" 

namespace pdb
{

class ColumnBuilder;

using GetDatumFunc = std::function<arrow::Status(arrow::Array*, int64_t, Datum*)>;

class ColumnExchanger {
public:
  ColumnExchanger(Oid rel, Form_pg_attribute attr);
  ColumnExchanger(int16_t typid);

  GetDatumFunc GetFunction(Oid rel, Form_pg_attribute attr);

  GetDatumFunc GetFunction(Oid typid);

  GetDatumFunc GetFunction(Oid typid,
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

private:
  GetDatumFunc func_;
  std::shared_ptr<arrow::Array> array_;
  Oid rel_;
};

}



