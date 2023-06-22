/*-------------------------------------------------------------------------
 *
 * modify_state.cpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2022, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/modify_state.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/access/parquet/modify_state.hpp"

#include <sys/time.h>

#include <functional>
#include <list>

extern "C" {
#include "executor/executor.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
}

/**
 * @brief Create a parquet modify state object
 *
 * @param reader_cxt memory context for reader
 * @param dirname directory path
 * @param s3_client aws s3 client
 * @param tuple_desc tuple descriptor
 * @param use_threads use_thread option
 * @param use_mmap use_mmap option
 * @return ParquetS3FdwModifyState* parquet modify state object
 */

ParquetS3ModifyState *create_parquet_modify_state(
    MemoryContext reader_cxt, const char *dirname, Aws::S3::S3Client *s3_client,
    TupleDesc tuple_desc, std::set<int> target_attrs, bool use_threads, bool use_mmap) {
  return new ParquetS3ModifyState(reader_cxt, dirname, s3_client, tuple_desc, target_attrs,
                                  use_threads, use_mmap);
}

/**
 * @brief Construct a new Parquet S 3 Fdw Modify State:: Parquet S 3 Fdw Modify
 * State object
 *
 * @param reader_cxt memory context for reader
 * @param dirname directory path
 * @param s3_client aws s3 client
 * @param tuple_desc tuple descriptor
 * @param use_threads use_thread option
 * @param use_mmap use_mmap option
 */
ParquetS3ModifyState::ParquetS3ModifyState(MemoryContext reader_cxt,
                                           const char *dirname,
                                           Aws::S3::S3Client *s3_client,
                                           TupleDesc tuple_desc,
										   std::set<int> target_attrs,
                                           bool use_threads, bool use_mmap)
    : cxt(reader_cxt),
      dirname(dirname),
      s3_client(s3_client),
      tuple_desc(tuple_desc),
      target_attrs(target_attrs),
      use_threads(use_threads),
      use_mmap(use_mmap),
      schemaless(schemaless) {}

/**
 * @brief Destroy the Parquet S 3 Fdw Modify State:: Parquet S3 Fdw Modify State
 * object
 */ ParquetS3ModifyState::~ParquetS3ModifyState() {}
/**
 * @brief add a parquet file
 *
 * @param filename file path
 */
/*
void ParquetS3ModifyState::add_file(uint64_t blockid, const char *filename, 
									std::shared_ptr<arrow::RecordBatch> recordbatch) {
  if (file_schema_ == nullptr) {
    file_schema_ = create_new_file_schema();
  }
  std::shared_ptr<ParquetWriter> reader =
      CreateParquetWriter(filename, tuple_desc, file_schema_);

  reader->Open(dirname, s3_client);
  // reader->SetRecordBatch(recordbatch);
  //reader->create_column_mapping(this->tuple_desc, this->target_attrs);
  //reader->set_options(use_threads, use_mmap);

  updates[blockid] = reader;
}

*/

/**
 * @brief add a new parquet file
 *
 * @param filename new file path
 * @param slot tuple table slot data
 * @return ParquetWriter* reader to new file
 */
std::shared_ptr<ParquetWriter> ParquetS3ModifyState::new_inserter(const char *filename,
                                                        TupleTableSlot *slot) {
  if (file_schema_ == nullptr) {
    file_schema_ = create_new_file_schema();
  }

  auto reader = CreateParquetWriter(filename, tuple_desc, file_schema_);
  //reader->Open(filename, s3_client);
  //reader->set_options(use_threads, use_mmap);

  /* create temporary file */
  //reader->create_column_mapping(this->tuple_desc, this->target_attrs);
  //reader->create_new_file_temp_cache();

  reader->PrepareUpload();

  return reader;
}

/**
 * @brief check aws s3 client is existed
 *
 * @return true if s3_client is existed
 */
bool ParquetS3ModifyState::has_s3_client() {
  if (this->s3_client) {
    return true;
  }
  return false;
}

/**
 * @brief upload all cached data on readers list
 */
void ParquetS3ModifyState::upload() {
  for (auto update : updates) {
    update.second->Upload(dirname, s3_client);
    uploads.push_back(update.second);
  }
  updates.clear();

  if (inserter != nullptr) {
    inserter->Upload(dirname, s3_client);
    uploads.push_back(inserter);
    inserter = nullptr;
  }

  for (auto upload : uploads) {
    upload->CommitUpload();
  }
}

/**
 * @brief insert a postgres tuple table slot to list parquet file
 *
 * @param slot tuple table slot
 * @return true if insert successfully
 */
bool ParquetS3ModifyState::exec_insert(TupleTableSlot *slot) {
  if (inserter != nullptr && inserter->DataSize() > 100 * 1024 * 0124) {
    inserter->Upload(dirname, s3_client);
    uploads.push_back(inserter);
    inserter = nullptr;
  }

  if (inserter == nullptr) {
    char uuid[1024];
    static uint32_t local_index = 0;
    uint64_t worker_uuid = 1;
    sprintf(uuid, "%d_%d.parquet", worker_uuid, local_index++);
    inserter = new_inserter(uuid, slot);
  }
  if (inserter != nullptr) {
    return inserter->ExecInsert(slot);
  }
  return false;
}

/**
 * @brief delete a record in list parquet file by key column
 *
 * @param slot tuple table slot
 * @param planSlot junk values
 * @return true if delete successfully
 */
bool ParquetS3ModifyState::exec_delete(ItemPointer tid) {
  uint64_t block_id = ItemPointerGetBlockNumber(tid);
  auto it = updates.find(block_id);
  if (it != updates.end()) {
    return it->second->ExecDelete(tid->ip_posid);
  }
  return false;
}

/**
 * @brief set relation name
 *
 * @param name relation name
 */
void ParquetS3ModifyState::set_rel_name(char *name) { this->rel_name = name; }

/**
 * @brief get arrow::DataType from given arrow type id
 *
 * @param type_id arrow type id
 * @return std::shared_ptr<arrow::DataType>
 */
static std::shared_ptr<arrow::DataType> to_primitive_DataType(
    arrow::Type::type type_id) {
  switch (type_id) {
    case arrow::Type::BOOL:
      return arrow::boolean();
    case arrow::Type::INT8:
      return arrow::int8();
    case arrow::Type::INT16:
      return arrow::int16();
    case arrow::Type::INT32:
      return arrow::int32();
    case arrow::Type::INT64:
      return arrow::int64();
    case arrow::Type::FLOAT:
      return arrow::float32();
    case arrow::Type::DOUBLE:
      return arrow::float64();
    case arrow::Type::DATE32:
      return arrow::date32();
    case arrow::Type::TIMESTAMP:
      return arrow::timestamp(arrow::TimeUnit::MICRO);
    default:
      return arrow::utf8(); /* all other type is convert as text */
  }
}

/**
 * @brief get arrow::DataType from given Jsonb value type
 *
 * @param jbv_type Jsonb value type
 * @return std::shared_ptr<arrow::DataType>
 */
static std::shared_ptr<arrow::DataType> jbvType_to_primitive_DataType(
    jbvType jbv_type) {
  switch (jbv_type) {
    case jbvNumeric:
      return arrow::float64();
    case jbvBool:
      return arrow::boolean();
    default:
      return arrow::utf8(); /* all other type is convert as text */
  }
}

/**
 * @brief parse schemaless/jsonb column
 *
 * @param[in] attr_value Jsonb Datum
 * @param[out] names parsed columns name
 * @param[out] values parsed columns value
 * @param[out] is_nulls parsed columns value isnull
 * @param[out] types parsed columns Jsonb type
 */
static void parse_jsonb_column(Datum attr_value,
                               std::vector<std::string> &names,
                               std::vector<Datum> &values,
                               std::vector<bool> &is_nulls,
                               std::vector<jbvType> &types) {
  Jsonb *jb = DatumGetJsonbP(attr_value);
  Datum *cols;
  Datum *col_vals;
  jbvType *col_types;
  bool *col_isnulls;
  size_t len;

  parquet_parse_jsonb(&jb->root, &cols, &col_vals, &col_types, &col_isnulls,
                      &len);

  for (size_t col_idx = 0; col_idx < len; col_idx++) {
    bytea *bytea_val = DatumGetByteaP(cols[col_idx]);
    size_t str_len = VARSIZE(bytea_val) - VARHDRSZ;
    char *str = (char *)palloc0(sizeof(char) * (str_len + 1));

    memcpy(str, VARDATA(bytea_val), str_len);
    names.push_back(str);
    values.push_back(col_vals[col_idx]);
    if (col_types[col_idx] == jbvNull)
      is_nulls.push_back(true);
    else
      is_nulls.push_back(false);

    types.push_back(col_types[col_idx]);
  }
}
/**
 * @brief Create base on column of inserted record and existed columns.
 *        If column is not exist on any file, create schema by mapping type.
 *
 * @param slot tuple table slot
 * @return std::shared_ptr<arrow::Schema> new file schema
 */
std::shared_ptr<arrow::Schema> ParquetS3ModifyState::create_new_file_schema() {
  arrow::FieldVector fields;
  int natts = this->tuple_desc->natts;
  bool *founds = (bool *)palloc0(sizeof(bool) * natts);
	
/*
  memset(founds, false, natts);
  for (auto reader : readers) {
    auto schema = reader->get_file_schema();
    for (int i = 0; i < natts; i++) {
      char pg_colname[NAMEDATALEN];
      Form_pg_attribute att = TupleDescAttr(this->tuple_desc, i);

      if (founds[i] == true || att->attisdropped) continue;

      tolowercase(NameStr(att->attname), pg_colname);
      auto field = schema->GetFieldByName(pg_colname);

      if (field != nullptr) {
        founds[i] = true;
        fields.push_back(field);
      }
    }
  }
  */

  for (int i = 0; i < natts; i++) {
    Form_pg_attribute att = TupleDescAttr(this->tuple_desc, i);
    arrow::Type::type type_id;

    if (att->attisdropped || founds[i] == true) continue;

    type_id = postgres_to_arrow_type(att->atttypid, att->atttypmod, att->attlen, att->attbyval);
    if (type_id != arrow::Type::NA) {
      fields.push_back(
          arrow::field(att->attname.data, to_primitive_DataType(type_id)));
	} else if (att->atttypid == JSONBOID) {
			elog(ERROR,
               "parquet_s3_fdw: can not create parquet mapping type for jsonb "
               "column: %s.",
               att->attname.data);
    } else {
      Oid elemtyp = get_element_type(att->atttypid);
	
      if (elemtyp == InvalidOid) {
				// FIXME_SDB
			elemtyp = att->atttypid;
	  }

      if (elemtyp != InvalidOid) {
        arrow::Type::type elem_type_id = postgres_to_arrow_type(att->atttypid, att->atttypmod, att->attlen, att->attbyval);
        if (elem_type_id != arrow::Type::NA)
          fields.push_back(
              arrow::field(att->attname.data,
                           arrow::list(to_primitive_DataType(elem_type_id))));
		else {
        elog(PANIC,
             "parquet to arrow type error: type %d , %d",
             att->atttypid, elemtyp);
		}
      } else {
        elog(PANIC,
             "parquet_s3_fdw: Can not create parquet mapping type for type "
             "OID: %d, column: %s.",
             att->atttypid, att->attname.data);
      }
    }
  }
  elog(WARNING, "create_new_file_schem, schema size: %d, should be(%d natts) ", fields.size(), natts);

  return arrow::schema(fields);
}
