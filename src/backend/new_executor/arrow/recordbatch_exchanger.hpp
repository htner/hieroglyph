#pragma once

#include "backend/new_executor/arrow/column_exchanger.hpp"
#include "backend/new_executor/arrow/recordbatch_exchanger.hpp"
#include "backend/new_executor/pg.hpp" 
#include "backend/sdb/common/pg_export.hpp"

#include <memory>
#include <arrow/record_batch.h>
#include <vector>

namespace pdb
{

class ColumnExchanger;

class RecordBatchExchanger {
public:
  RecordBatchExchanger(Oid rel, TupleDesc tuple_desc, const std::vector<bool>& fetched_col);
  RecordBatchExchanger(const RecordBatchExchanger&) = delete;
  RecordBatchExchanger& operator= (const RecordBatchExchanger&) = delete;

  ~RecordBatchExchanger();

  void SetRecordBatch(std::shared_ptr<arrow::RecordBatch> batch);

  arrow::Result<TupleTableSlot*> FetchNextTuple();
  arrow::Result<TupleTableSlot*> FetchTuple(uint32_t);

private:
  Oid rel_;
  int64_t index_= 0;
  TupleTableSlot* slot_;
  TupleDesc					 tuple_desc_;
  std::shared_ptr<arrow::RecordBatch> batch_;
  std::vector<std::shared_ptr<ColumnExchanger> > column_exchangers_; 
};

}  // namespace pdb
