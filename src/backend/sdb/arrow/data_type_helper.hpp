#pragma once

#include <memory>
#include <arrow/type.h>

#include "backend/sdb/common/pg_export.hpp" 

namespace sdb
{

class DataTypeHelper {
public:	
  DataTypeHelper() = default;

  static std::shared_ptr<arrow::DataType> GetBaseDataType(Oid typid, 
                                                   int32_t typlen,
                                                   int32_t typmod);

  static std::shared_ptr<arrow::DataType> GetDataType(Oid typid, 
                                               Oid typelem,
											                         Oid typrelid,
                                               int32_t typlen,
                                               int32_t typmod,
                                               char typtype);

  static std::shared_ptr<arrow::DataType> GetDataType(Form_pg_attribute);

  static std::shared_ptr<arrow::DataType> GetDataType(Oid typid);
};

}
