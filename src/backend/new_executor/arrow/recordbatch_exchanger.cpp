#include "backend/new_executor/arrow/recordbatch_exchanger.hpp"
#include "backend/sdb/common/pg_export.hpp"
//#include "access/tupdesc.h"
#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <butil/logging.h>

namespace pdb {

RecordBatchExchanger::RecordBatchExchanger(Oid rel, TupleDesc tuple_desc) {
  slot_ = MakeSingleTupleTableSlot(tuple_desc, &TTSOpsVirtual);
  tuple_desc_ = CreateTupleDescCopy(tuple_desc);
  //LOG(WARNING, "-- ------------------ rel %d natts :%d", rel, tuple_desc->natts);
  for (int i = 0; i < tuple_desc->natts; ++i) {
    Form_pg_attribute att = TupleDescAttr(tuple_desc_, i);        
	// std::shared_ptr<DataType> data_type = TypeMapping(att);
	auto column_exchanger = std::make_shared<ColumnExchanger>(rel, att); 
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

int kRow__ = 0;
arrow::Result<TupleTableSlot*> RecordBatchExchanger::FetchNextTuple() {
	ExecClearTuple(slot_);
	for (int i = 0; i < tuple_desc_->natts; ++i) {
		kRow__ = i;
		bool* isnull = &(slot_->tts_isnull[i]);
		Datum* datum = &(slot_->tts_values[i]);	
		arrow::Status status = column_exchangers_[i]->GetDatumByIndex(index_, datum, isnull);
		if (!status.ok()) {
			//elog(WARNING, "fetch next tuple finish, index:%d, natt:%d", index_, i);
			return status;
		}
	}
//	LOG(ERROR) << "attr: 1 -> "<< DatumGetUInt32(slot_->tts_values[0]);
	// elog(WARNING, "fetch next tuple, index %d", index_);
	++index_;
	return ExecStoreVirtualTuple(slot_);
}

}
