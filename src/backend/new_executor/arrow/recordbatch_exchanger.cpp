#include "backend/new_executor/arrow/recordbatch_exchanger.hpp"
#include "backend/sdb/common/pg_export.hpp"
//#include "access/tupdesc.h"
#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/iobuf.h>
#include <butil/logging.h>

namespace pdb {

RecordBatchExchanger::RecordBatchExchanger(Oid rel, TupleDesc tuple_desc,
						 	const std::vector<bool>& fetched_col) {
  slot_ = MakeSingleTupleTableSlot(tuple_desc, &TTSOpsVirtual);
  tuple_desc_ = CreateTupleDescCopy(tuple_desc);
  //LOG(ERROR) << "-- ------------------ rel %d natts :%d" << rel << " " << tuple_desc->natts;
  Assert(fetched_col.size() == tuple_desc->natts);

  for (int i = 0; i < tuple_desc->natts; ++i) {
	if (fetched_col[i]) {
		Form_pg_attribute att = TupleDescAttr(tuple_desc_, i);        
		// std::shared_ptr<DataType> data_type = TypeMapping(att);
		auto column_exchanger = std::make_shared<ColumnExchanger>(rel, att); 
		column_exchangers_.push_back(column_exchanger);
	} else {
		column_exchangers_.push_back(nullptr);
	}
  }
  //LOG(ERROR) << "2 -- ------------------ rel %d natts :%d"<< rel << " " << tuple_desc->natts;
}

RecordBatchExchanger::~RecordBatchExchanger() {
	FreeTupleDesc(tuple_desc_);
}

void RecordBatchExchanger::SetRecordBatch(std::shared_ptr<arrow::RecordBatch> batch) {
	batch_ = batch;
	index_ = 0;
	for (size_t i = 0; i < column_exchangers_.size(); ++i) {
		if (column_exchangers_[i] != nullptr) {
			column_exchangers_[i]->SetArray(batch_->column(i));
		}
	}
}

arrow::Result<TupleTableSlot*> RecordBatchExchanger::FetchNextTuple() {
	ExecClearTuple(slot_);
	for (int i = 0; i < tuple_desc_->natts; ++i) {
		if (!column_exchangers_[i]) {
			slot_->tts_isnull[i] = true;
			continue;
		}
		//LOG(ERROR)<< "fetch next tuple finish, index:%d, natt:%d" << index_ << i;
		bool* isnull = &(slot_->tts_isnull[i]);
		Datum* datum = &(slot_->tts_values[i]);	
		arrow::Status status = column_exchangers_[i]->GetDatumByIndex(index_, datum, isnull);
		if (!status.ok()) {
			return status;
		}
	}
	//LOG(WARNING) << "fetch next tuple, index " << index_;
	ItemPointerSetOffsetNumber(&(slot_->tts_tid), (uint32_t)index_);
	++index_;
	return ExecStoreVirtualTuple(slot_);
}

arrow::Result<TupleTableSlot*> RecordBatchExchanger::FetchTuple(uint32 index) {
	ExecClearTuple(slot_);
	for (int i = 0; i < tuple_desc_->natts; ++i) {
		if (!column_exchangers_[i]) {
			slot_->tts_isnull[i] = true;
			continue;
		}
		bool* isnull = &(slot_->tts_isnull[i]);
		Datum* datum = &(slot_->tts_values[i]);	
		arrow::Status status = column_exchangers_[i]->GetDatumByIndex(index, datum, isnull);
		if (!status.ok()) {
			LOG(ERROR) << "fetch next tuple finish error, index: " << index_ << ", natt:" << i;
			return status;
		}
	}
	//elog(WARNING, "fetch tuple, index %d", index);
	ItemPointerSetOffsetNumber(&(slot_->tts_tid), (uint16_t)index);
	return ExecStoreVirtualTuple(slot_);
}

}
