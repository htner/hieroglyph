#include "backend/new_executor/arrow/recordbatch_exchanger.hpp"

#include "backend/new_executor/arrow/column_exchanger.hpp"
//#include "access/tupdesc.h"

namespace pdb {

RecordBatchExchanger::RecordBatchExchanger(TupleDesc tuple_desc) {
  slot_ = MakeSingleTupleTableSlot(tuple_desc, &TTSOpsVirtual);
  tuple_desc_ = CreateTupleDescCopy(tuple_desc);
  for (int i = 0; i < tuple_desc->natts; ++i) {
    Form_pg_attribute att = TupleDescAttr(tuple_desc_, i);        
	// std::shared_ptr<DataType> data_type = TypeMapping(att);
	auto column_exchanger = std::make_shared<ColumnExchanger>(att); 
	column_exchangers_.push_back(column_exchanger);
  }
}

RecordBatchExchanger::~RecordBatchExchanger() {
	FreeTupleDesc(tuple_desc_);
}

void RecordBatchExchanger::SetRecordBatch(std::shared_ptr<arrow::RecordBatch> batch) {
	batch_ = batch;
	index_ = 0;
	for (size_t i = 0; i < column_exchangers_.size(); ++i) {
		column_exchangers_[i]->SetArray(batch_->column(i));
	}
}

arrow::Result<TupleTableSlot*> RecordBatchExchanger::FetchNextTuple() {
	//
	ExecClearTuple(slot_);
	for (int i = 0; i < tuple_desc_->natts; ++i) {
		bool* isnull = &(slot_->tts_isnull[i]);
		Datum* datum = &(slot_->tts_values[i]);	
		arrow::Status status = column_exchangers_[i]->GetDatumByIndex(index_, datum, isnull);
		if (!status.ok()) {
			elog(WARNING, "fetch next tuple finish, index:%d, natt:%d", index_, i);
			return status;
		}
	}
	elog(WARNING, "fetch next tuple, index %d", index_);
	++index_;

	return ExecStoreVirtualTuple(slot_);
}

}
