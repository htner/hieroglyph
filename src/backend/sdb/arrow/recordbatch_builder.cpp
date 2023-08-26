#include "backend/sdb/arrow/recordbatch_builder.hpp"
#include <butil/logging.h>

namespace sdb {

RecordBatchBuilder::RecordBatchBuilder(Oid rel, TupleDesc tuple_desc) {
	tuple_desc_ = CreateTupleDescCopy(tuple_desc);
	arrow::FieldVector fields;

	for (int i = 0; i < tuple_desc_->natts; ++i) {
		Form_pg_attribute att = TupleDescAttr(tuple_desc_, i);        
		if (att->atttypid == 0) {
			LOG(ERROR) << "typid == 0";
		}
		auto builder = std::make_unique<ArrayBuilder>(rel, att);

		auto array_builder = builder->GetArrayBuilder();
		std::shared_ptr<arrow::Field> field = 
			std::make_shared<arrow::Field>(NameStr(att->attname), array_builder->type());
		fields.push_back(field);

		builders_.push_back(std::move(builder));
	}
	schema_ = std::make_shared<arrow::Schema>(fields);
}

RecordBatchBuilder::~RecordBatchBuilder() {
	FreeTupleDesc(tuple_desc_);
}

arrow::Status RecordBatchBuilder::AppendTuple(TupleTableSlot* tuple) {
	for (int i = 0; i < tuple_desc_->natts; i++) {
		try {
			auto status = builders_[i]->AppendDatum(tuple->tts_values[i], tuple->tts_isnull[i]);
			if (!status.ok()) {
				return status;
			}
		} catch (const std::exception &e) {
			LOG(ERROR) << "record batch : " << e.what();
			return arrow::Status::UnknownError("");
		}
	}
	return arrow::Status::OK();
}

std::shared_ptr<arrow::RecordBatch> RecordBatchBuilder::Finish() {
	std::vector<std::shared_ptr<arrow::Array>> columns;
	for (size_t i = 0; i < builders_.size(); ++i) {
		std::shared_ptr<arrow::Array> arr;
		auto status = builders_[i]->GetArrayBuilder()->Finish(&arr);
		if (!status.ok()) {
			LOG(ERROR) << "finish error, col %d size %d" <<  i << " " << arr->length();
			return nullptr;
		}
		columns.push_back(arr);
		builders_[i]->GetArrayBuilder()->Reset();
	}
	if (columns[0]->length() == 0) {
		return nullptr;
	}
	return arrow::RecordBatch::Make(schema_, columns[0]->length(), columns);
}

} // namespace pdb
