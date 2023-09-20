#pragma once

#include "backend/sdb/arrow/array_to_scalar_exchanger.hpp"
//#include "backend/sdb/arrow/recordbatch_exchanger.hpp"
#include "backend/sdb/common/pg_export.hpp"

#include <memory>
#include <arrow/record_batch.h>
#include <vector>

namespace sdb
{

class ArraySalarExchanger;

class RecordBatchSlotExchanger {
public:
  RecordBatchSlotExchanger(Oid rel, TupleDesc tuple_desc, const std::vector<bool>& fetched_col);
  RecordBatchSlotExchanger(const RecordBatchSlotExchanger&) = delete;
  RecordBatchSlotExchanger& operator= (const RecordBatchSlotExchanger&) = delete;

  ~RecordBatchSlotExchanger();

  void SetRecordBatch(std::shared_ptr<arrow::RecordBatch> batch);

  arrow::Result<TupleTableSlot*> FetchNextTuple();
  arrow::Result<TupleTableSlot*> FetchTuple(uint32_t);

private:
  Oid rel_;
  int64_t index_= 0;
  TupleTableSlot* slot_;
  TupleDesc					 tuple_desc_;
  std::shared_ptr<arrow::RecordBatch> batch_;
  //std::unordered_map<std::string, std::shared_ptr<ArraySalarExchanger> > column_exchangers_; 
  std::vector<std::shared_ptr<ArraySalarExchanger> > column_exchangers_; 
};

}  // namespace pdb
