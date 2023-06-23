#pragma once

#include <memory>
#include <functional>

#include <arrow/status.h>
#include <arrow/builder.h>

#include "backend/new_executor/pg.hpp"

namespace pdb
{

class ColumnBuilder;

using PutDatumFunc = std::function<arrow::Status(arrow::ArrayBuilder*, Datum, bool)>;

class ColumnBuilder {
public:
  ColumnBuilder(Form_pg_attribute attr);
//  ColumnBuilder(int16_t typid);

  arrow::Status AppendDatum(Datum d, bool isnull) {
  if (isnull) {
    return array_builder_->AppendNull();
  }
    return put_value_func_(array_builder_.get(), d, isnull);
  }

  PutDatumFunc GetPutValueFunction(Form_pg_attribute attr);
  PutDatumFunc GetPutValueFunction(Oid typid);
  PutDatumFunc GetPutValueFunction(Oid typid,
                                   int32_t typlen,
                                   char typtype,
                                   int32_t typmod,
                                   Oid typelem,
                                   Oid typrelid);
	

  arrow::Status SetPutValueFunction();

  arrow::ArrayBuilder* GetArrayBuilder();

  int64_t Size() {
    if (array_builder_ == nullptr) {
      return 0;
    }
    return array_builder_->length();
  }

private:
	Oid atttypid_;
	int32 atttypmod_;
	char typtype_;
  std::shared_ptr<arrow::DataType> arrow_type_;
  std::unique_ptr<arrow::ArrayBuilder> array_builder_;
  std::vector<ColumnBuilder> sub_column_builders_;
  PutDatumFunc put_value_func_;
};

}
