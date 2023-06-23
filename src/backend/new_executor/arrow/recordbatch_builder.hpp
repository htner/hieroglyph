#pragma once

#include "backend/new_executor/pg.hpp"

#include <memory>
#include <vector>

#include <arrow/status.h>
#include <arrow/record_batch.h>

#include "backend/new_executor/arrow/column_builder.hpp"

namespace pdb
{

class RecordBatchBuilder {
public:
  RecordBatchBuilder(TupleDesc tuple_desc);
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
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::unique_ptr<ColumnBuilder> > builders_; 
  TupleDesc tuple_desc_;
};

}
