#pragma once

#include "backend/sdb/common/pg_export.hpp"

#include <memory>
#include <vector>

#include <arrow/status.h>
#include <arrow/record_batch.h>

#include "backend/sdb/arrow/array_builder.hpp"

namespace sdb
{

class RecordBatchBuilder {
public:
  RecordBatchBuilder(Oid rel, TupleDesc tuple_desc);
  ~RecordBatchBuilder();

  RecordBatchBuilder(const RecordBatchBuilder&) = delete;
  RecordBatchBuilder& operator= (const RecordBatchBuilder&) = delete;

  arrow::Status AppendTuple(TupleTableSlot* slot);
  std::shared_ptr<arrow::RecordBatch> Finish();

  int64_t Size() {
    if (builders_.empty()) {
      return 0;
    }
    return builders_[0]->Size();
  }


private:
  Oid rel_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::unique_ptr<ArrayBuilder> > builders_; 
  TupleDesc tuple_desc_;
};

}
