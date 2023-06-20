/*-------------------------------------------------------------------------
 *
 * parquet_impl.cpp
 *		  Parquet processing implementation for parquet_s3_fdw
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/parquet_impl.cpp
 *
 *-------------------------------------------------------------------------
 */
// basename comes from string.h on Linux,
// but from libgen.h on other POSIX systems (see man basename)
#ifndef GNU_SOURCE
#include <libgen.h>
#endif

#include <math.h>
#include <sys/stat.h>

#include <list>
#include <set>

#include "arrow/api.h"
#include "arrow/array.h"
#include "arrow/io/api.h"
#include "backend/access/parquet/common.hpp"
#include "backend/access/parquet/exec_state.hpp"
#include "backend/access/parquet/heap.hpp"
#include "backend/access/parquet/modify_reader.hpp"
#include "backend/access/parquet/modify_state.hpp"
#include "backend/access/parquet/parquet_s3/parquet_s3.hpp"
#include "backend/access/parquet/reader.hpp"
#include "backend/access/parquet/slvars.hpp"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

extern "C" {
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/parallel.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/appendinfo.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#else
#include "access/relation.h"
#include "access/table.h"
#include "optimizer/optimizer.h"
#endif

#if PG_VERSION_NUM < 110000
#include "catalog/pg_am.h"
#else
#include "catalog/pg_am_d.h"
#endif
}

/* from costsize.c */
#define LOG2(x) (log(x) / 0.693147180559945)

#if PG_VERSION_NUM < 110000
#define PG_GETARG_JSONB_P PG_GETARG_JSONB
#endif

#define IS_KEY_COLUMN(A) \
  ((strcmp(def->defname, "key") == 0) && (defGetBoolean(def) == true))

extern bool enable_multifile;
extern bool enable_multifile_merge;

extern void parquet_s3_init();

static void find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2);
static void destroy_parquet_state(void *arg);
static List *parse_attributes_list(char *start);
/*
 * Restriction
 */
struct ParquetScanDescData {
  TableScanDescData rs_base;
  ParquetS3AccessState *state;
};

typedef struct ParquetScanDescData *ParquetScanDesc;

struct RowGroupFilter {
  AttrNumber attnum;
  bool is_key; /* for maps */
  Const *value;
  int strategy;
  char *attname;  /* actual column name in schemales mode */
  Oid atttype;    /* Explicit cast type in schemaless mode
                      In non-schemaless NULL is expectation  */
  bool is_column; /* for schemaless actual column `exist` operator */
};

static std::unordered_map<Oid, ParquetS3ModifyState*> fmstates;

ParquetS3ModifyState*
GetModifyState(Relation rel) {
	Oid oid = rel->rd_id;
	auto it = fmstates.find(oid);
	if (it != fmstates.end()) {
		return it->second;
	}
	return nullptr;
}

ParquetS3ModifyState*
CreateParquetModifyState(Relation rel,
						 MemoryContext ctx, 
						 char* dirname, 
						 Aws::S3::S3Client* s3client,
						 TupleDesc tuple_desc,
						 bool use_threads) {
	Oid oid = rel->rd_id;
	auto fmstate = GetModifyState(rel);	
	if (fmstate != NULL) {
		return fmstate;
	}
	std::set<int> attrs;
    fmstate = create_parquet_modify_state(
        ctx, dirname, s3client, tuple_desc, attrs, use_threads, true);
	fmstates[oid] = fmstate;
	return fmstate;
}

static Const *convert_const(Const *c, Oid dst_oid) {
  Oid funcid;
  CoercionPathType ct;

  ct = find_coercion_pathway(dst_oid, c->consttype, COERCION_EXPLICIT, &funcid);
  switch (ct) {
    case COERCION_PATH_FUNC: {
      FmgrInfo finfo;
      Const *newc;
      int16 typlen;
      bool typbyval;

      get_typlenbyval(dst_oid, &typlen, &typbyval);

      newc = makeConst(dst_oid, 0, c->constcollid, typlen, 0, c->constisnull,
                       typbyval);
      fmgr_info(funcid, &finfo);
      newc->constvalue = FunctionCall1(&finfo, c->constvalue);

      return newc;
    }
    case COERCION_PATH_RELABELTYPE:
      /* Cast is not needed */
      break;
    case COERCION_PATH_COERCEVIAIO: {
      /*
       * In this type of cast we need to output the value to a string
       * and then feed this string to the input function of the
       * target type.
       */
      Const *newc;
      int16 typlen;
      bool typbyval;
      Oid input_fn, output_fn;
      Oid input_param;
      bool isvarlena;
      // char   *str; FIXME

      /* Construct a new Const node */
      get_typlenbyval(dst_oid, &typlen, &typbyval);
      newc = makeConst(dst_oid, 0, c->constcollid, typlen, 0, c->constisnull,
                       typbyval);

      /* Get IO functions */
      getTypeOutputInfo(c->consttype, &output_fn, &isvarlena);
      getTypeInputInfo(dst_oid, &input_fn, &input_param);

      /* FIXME
str = DatumGetCString(OidOutputFunctionCall(output_fn,
                                  c->constvalue));
newc->constvalue = OidInputFunctionCall(input_fn, str,
                              input_param, 0);
                                                                                      */

      return newc;
    }
    default:
      elog(ERROR, "parquet_s3_fdw: cast function to %s is not found",
           format_type_be(dst_oid));
  }
  return c;
}

/*
 * row_group_matches_filter
 *      Check if min/max values of the column of the row group match filter.
 */
static bool row_group_matches_filter(parquet::Statistics *stats,
                                     const arrow::DataType *arrow_type,
                                     RowGroupFilter *filter) {
  FmgrInfo finfo;
  Datum val;
  int collid = filter->value->constcollid;
  int strategy = filter->strategy;

  if (arrow_type->id() == arrow::Type::MAP && filter->is_key) {
    /*
     * Special case for jsonb `?` (exists) operator. As key is always
     * of text type we need first convert it to the target type (if needed
     * of course).
     */

    /*
     * Extract the key type (we don't check correctness here as we've
     * already done this in `extract_rowgroups_list()`)
     */
    auto strct = arrow_type->fields()[0];
    auto key = strct->type()->fields()[0];
    arrow_type = key->type().get();

    /* Do conversion */
    filter->value =
        convert_const(filter->value, to_postgres_type(arrow_type->id()));
  }
  val = filter->value->constvalue;

  find_cmp_func(&finfo, filter->value->consttype,
                to_postgres_type(arrow_type->id()));

  switch (filter->strategy) {
    case BTLessStrategyNumber:
    case BTLessEqualStrategyNumber: {
      Datum lower;
      int cmpres;
      bool satisfies;
      std::string min = std::move(stats->EncodeMin());

      lower = bytes_to_postgres_type(min.c_str(), min.length(), arrow_type);
      cmpres = FunctionCall2Coll(&finfo, collid, val, lower);

      satisfies = (strategy == BTLessStrategyNumber && cmpres > 0) ||
                  (strategy == BTLessEqualStrategyNumber && cmpres >= 0);

      if (!satisfies) return false;
      break;
    }

    case BTGreaterStrategyNumber:
    case BTGreaterEqualStrategyNumber: {
      Datum upper;
      int cmpres;
      bool satisfies;
      std::string max = std::move(stats->EncodeMax());

      upper = bytes_to_postgres_type(max.c_str(), max.length(), arrow_type);
      cmpres = FunctionCall2Coll(&finfo, collid, val, upper);

      satisfies = (strategy == BTGreaterStrategyNumber && cmpres < 0) ||
                  (strategy == BTGreaterEqualStrategyNumber && cmpres <= 0);

      if (!satisfies) return false;
      break;
    }

    case BTEqualStrategyNumber:
    case JsonbExistsStrategyNumber: {
      Datum lower, upper;
      std::string min = std::move(stats->EncodeMin());
      std::string max = std::move(stats->EncodeMax());

      lower = bytes_to_postgres_type(min.c_str(), min.length(), arrow_type);
      upper = bytes_to_postgres_type(max.c_str(), max.length(), arrow_type);

      int l = FunctionCall2Coll(&finfo, collid, val, lower);
      int u = FunctionCall2Coll(&finfo, collid, val, upper);

      if (l < 0 || u > 0) return false;
      break;
    }

    default:
      /* should not happen */
      Assert(false);
  }

  return true;
}

typedef enum { PS_START = 0, PS_IDENT, PS_QUOTE } ParserState;

static bool parquet_s3_column_is_existed(
    parquet::arrow::SchemaManifest manifest, char *column_name) {
  for (auto &schema_field : manifest.schema_fields) {
    auto &field = schema_field.field;
    char arrow_colname[NAMEDATALEN];

    if (field->name().length() > NAMEDATALEN - 1) {
      throw Error("parquet column name '%s' is too long (max: %d)",
                  field->name().c_str(), NAMEDATALEN - 1);
    }
    tolowercase(field->name().c_str(), arrow_colname);

    if (strcmp(column_name, arrow_colname) == 0) {
      return true; /* Found!!! */
    }
  }

  /* Can not found column from parquet file */
  return false;
}

/*
 * extract_rowgroups_list
 *      Analyze query predicates and using min/max statistics determine which
 *      row groups satisfy clauses. Store resulting row group list to
 *      fdw_private.
 */
List *extract_rowgroups_list(const char *filename, const char *dirname,
                             Aws::S3::S3Client *s3_client, TupleDesc tupleDesc,
                             std::list<RowGroupFilter> &filters,
                             uint64 *matched_rows, uint64 *total_rows,
                             bool schemaless) noexcept {
  std::unique_ptr<parquet::arrow::FileReader> reader;
  arrow::Status status;
  List *rowgroups = NIL;
  ReaderCacheEntry *reader_entry = NULL;
  std::string error;

  /* Open parquet file to read meta information */
  try {
    if (s3_client) {
      char *dname;
      char *fname;
      parquetSplitS3Path(dirname, filename, &dname, &fname);
      reader_entry = parquetGetFileReader(s3_client, dname, fname);
      reader = std::move(reader_entry->file_reader->reader);
      pfree(dname);
      pfree(fname);
    } else {
      status = parquet::arrow::FileReader::Make(
          arrow::default_memory_pool(),
          parquet::ParquetFileReader::OpenFile(filename, false), &reader);
    }

    if (!status.ok())
      throw Error("parquet_impl extract_rowgroups_list: failed to open Parquet file %s",
                  status.message().c_str());

    auto meta = reader->parquet_reader()->metadata();
    parquet::ArrowReaderProperties props;
    parquet::arrow::SchemaManifest manifest;

    status = parquet::arrow::SchemaManifest::Make(meta->schema(), nullptr,
                                                  props, &manifest);
    if (!status.ok())
      throw Error("parquet_s3_fdw: error creating arrow schema");

    /* Check each row group whether it matches the filters */
    for (int r = 0; r < reader->num_row_groups(); r++) {
      bool match = true;
      auto rowgroup = meta->RowGroup(r);

      /* Skip empty rowgroups */
      if (!rowgroup->num_rows()) continue;

      for (auto &filter : filters) {
        AttrNumber attnum;
        char pg_colname[NAMEDATALEN];

        if (schemaless) {
          /* In schemaless mode, attname has already existed  */
          tolowercase(filter.attname, pg_colname);

          if (filter.is_column == true) {
            /*
             * Check column existed for condition: v ? column
             * If column is not existed, exclude current file from file list.
             */
            if ((match = parquet_s3_column_is_existed(manifest, pg_colname)) ==
                false) {
              elog(DEBUG1, "parquet_s3_fdw: skip file %s", filename);
              return NIL;
            }
            continue;
          }
        } else {
          attnum = filter.attnum - 1;
          tolowercase(NameStr(TupleDescAttr(tupleDesc, attnum)->attname),
                      pg_colname);
        }

        /*
         * Search for the column with the same name as filtered attribute
         */
        for (auto &schema_field : manifest.schema_fields) {
          MemoryContext ccxt = CurrentMemoryContext;
          bool error = false;
          char errstr[ERROR_STR_LEN];
          char arrow_colname[NAMEDATALEN];
          auto &field = schema_field.field;
          int column_index;

          /* Skip complex objects (lists, structs except maps) */
          if (schema_field.column_index == -1 &&
              field->type()->id() != arrow::Type::MAP)
            continue;

          if (field->name().length() > NAMEDATALEN - 1)
            throw Error("parquet column name '%s' is too long (max: %d)",
                        field->name().c_str(), NAMEDATALEN - 1);
          tolowercase(field->name().c_str(), arrow_colname);

          if (strcmp(pg_colname, arrow_colname) != 0) continue;

          /* in schemaless mode, skip filter if parquet column type is not match
           * with actual column (explicit cast) type */
          if (schemaless) {
            int arrow_type = field->type().get()->id();

            if (!(filter.atttype == to_postgres_type(arrow_type) ||
                  (filter.atttype == JSONBOID &&
                   arrow_type == arrow::Type::MAP)))
              continue;
          }

          if (field->type()->id() == arrow::Type::MAP) {
            /*
             * Extract `key` column of the map.
             * See `create_column_mapping()` for some details on
             * map structure.
             */
            Assert(schema_field.children.size() == 1);
            auto &strct = schema_field.children[0];

            Assert(strct.children.size() == 2);
            auto &key = strct.children[0];
            column_index = key.column_index;
          } else
            column_index = schema_field.column_index;

          /* Found it! */
          std::shared_ptr<parquet::Statistics> stats;
          auto column = rowgroup->ColumnChunk(column_index);
          stats = column->statistics();

          PG_TRY();
          {
            /*
             * If at least one filter doesn't match rowgroup exclude
             * the current row group and proceed with the next one.
             */
            if (stats && !row_group_matches_filter(
                             stats.get(), field->type().get(), &filter)) {
              match = false;
              elog(DEBUG1, "parquet_s3_fdw: skip rowgroup %d", r + 1);
            }
          }
          PG_CATCH();
          {
            ErrorData *errdata;

            MemoryContextSwitchTo(ccxt);
            error = true;
            errdata = CopyErrorData();
            FlushErrorState();

            strncpy(errstr, errdata->message, ERROR_STR_LEN - 1);
            FreeErrorData(errdata);
          }
          PG_END_TRY();
          if (error)
            throw Error("parquet_s3_fdw: row group filter match failed: %s",
                        errstr);
          break;
        } /* loop over columns */

        if (!match) break;

      } /* loop over filters */

      /* All the filters match this rowgroup */
      if (match) {
        /* TODO: PG_TRY */
        rowgroups = lappend_int(rowgroups, r);
        *matched_rows += rowgroup->num_rows();
      }
      *total_rows += rowgroup->num_rows();
    } /* loop over rowgroups */
  } catch (const std::exception &e) {
    error = e.what();
  }
  if (!error.empty()) {
    if (reader_entry) reader_entry->file_reader->reader = std::move(reader);
    elog(ERROR,
         "parquet_s3_fdw: failed to exctract row groups from Parquet file: %s",
         error.c_str());
  }

  return rowgroups;
}

struct FieldInfo {
  char name[NAMEDATALEN];
  Oid oid;
};

/*
 * extract_parquet_fields
 *      Read parquet file and return a list of its fields
 */
List *extract_parquet_fields(const char *path, const char *dirname,
                             Aws::S3::S3Client *s3_client) noexcept {
  List *res = NIL;
  std::string error;

  try {
    std::unique_ptr<parquet::arrow::FileReader> reader;
    parquet::ArrowReaderProperties props;
    parquet::arrow::SchemaManifest manifest;
    arrow::Status status;
    FieldInfo *fields;

    if (s3_client) {
      arrow::MemoryPool *pool = arrow::default_memory_pool();
      char *dname;
      char *fname;
      parquetSplitS3Path(dirname, path, &dname, &fname);
      std::shared_ptr<arrow::io::RandomAccessFile> input(
          new S3RandomAccessFile(s3_client, dname, fname));
      status = parquet::arrow::OpenFile(input, pool, &reader);
      pfree(dname);
      pfree(fname);
    } else {
      status = parquet::arrow::FileReader::Make(
          arrow::default_memory_pool(),
          parquet::ParquetFileReader::OpenFile(path, false), &reader);
    }
    if (!status.ok())
      throw Error("parquet_impl extract_parquet_fields: failed to open Parquet file %s",
                  status.message().c_str());

    auto p_schema = reader->parquet_reader()->metadata()->schema();
    if (!parquet::arrow::SchemaManifest::Make(p_schema, nullptr, props,
                                              &manifest)
             .ok())
      throw std::runtime_error("parquet_s3_fdw: error creating arrow schema");

    fields = (FieldInfo *)exc_palloc(sizeof(FieldInfo) *
                                     manifest.schema_fields.size());

    for (auto &schema_field : manifest.schema_fields) {
      auto &field = schema_field.field;
      auto &type = field->type();
      Oid pg_type;

      switch (type->id()) {
        case arrow::Type::LIST: {
          arrow::Type::type subtype_id;
          Oid pg_subtype;
          bool error = false;

          if (type->num_fields() != 1)
            throw std::runtime_error("lists of structs are not supported");

          subtype_id = get_arrow_list_elem_type(type.get());
          pg_subtype = to_postgres_type(subtype_id);

          /* This sucks I know... */
          PG_TRY();
          { pg_type = get_array_type(pg_subtype); }
          PG_CATCH();
          { error = true; }
          PG_END_TRY();

          if (error)
            throw std::runtime_error(
                "failed to get the type of array elements");
          break;
        }
        case arrow::Type::MAP:
          pg_type = JSONBOID;
          break;
        default:
          pg_type = to_postgres_type(type->id());
      }

      if (pg_type != InvalidOid) {
        if (field->name().length() > 63)
          throw Error("parquet_s3_fdw: field name '%s' in '%s' is too long",
                      field->name().c_str(), path);

        memcpy(fields->name, field->name().c_str(), field->name().length() + 1);
        fields->oid = pg_type;
        res = lappend(res, fields++);
      } else {
        throw Error(
            "parquet_s3_fdw: cannot convert field '%s' of type '%s' in %s",
            field->name().c_str(), type->name().c_str(), path);
      }
    }
  } catch (std::exception &e) {
    error = e.what();
  }
  if (!error.empty()) elog(ERROR, "parquet_s3_fdw: %s", error.c_str());

  return res;
}

static void destroy_parquet_state(void *arg) {
  ParquetS3AccessState *festate = (ParquetS3AccessState *)arg;

  if (festate) {
    delete festate;
  }
}

static void destroy_parquet_modify_state(void *arg) {
  ParquetS3ModifyState *fmstate = (ParquetS3ModifyState *)arg;

  if (fmstate && fmstate->has_s3_client()) {
    /*
     * After modify, parquet file information on S3 server is different with
     * cached one, so, disable connection imediately after modify to reload this
     * infomation.
     */
    parquet_disconnect_s3_server();
    delete fmstate;
  }
}

/*
 * C interface functions
 */

static List *parse_attributes_list(char *start) {
  List *attrs = NIL;
  char *token;
  const char *delim = " ";

  while ((token = strtok(start, delim)) != NULL) {
    attrs = lappend(attrs, pstrdup(token));
    start = NULL;
  }

  return attrs;
}

/*
 * OidFunctionCall1NullableArg
 *      Practically a copy-paste from FunctionCall1Coll with added capability
 *      of passing a NULL argument.
 */
static Datum OidFunctionCall1NullableArg(Oid functionId, Datum arg,
                                         bool argisnull) {
#if PG_VERSION_NUM < 120000
  FunctionCallInfoData _fcinfo;
  FunctionCallInfoData *fcinfo = &_fcinfo;
#else
  LOCAL_FCINFO(fcinfo, 1);
#endif
  FmgrInfo flinfo;
  Datum result;

  fmgr_info(functionId, &flinfo);
  InitFunctionCallInfoData(*fcinfo, &flinfo, 1, InvalidOid, NULL, NULL);

#if PG_VERSION_NUM < 120000
  fcinfo->arg[0] = arg;
  fcinfo->argnull[0] = false;
#else
  fcinfo->args[0].value = arg;
  fcinfo->args[0].isnull = argisnull;
#endif

  result = FunctionCallInvoke(fcinfo);

  /* Check for null result, since caller is clearly not expecting one */
  if (fcinfo->isnull)
    elog(ERROR, "parquet_s3_fdw: function %u returned NULL", flinfo.fn_oid);

  return result;
}

/*
 * OidFunctionCallnNullableArg
 *      Practically a copy-paste from FunctionCall2Coll with added capability
 *      of passing a NULL argument.
 */
static Datum OidFunctionCallnNullableArg(Oid functionId, Datum *args,
                                         bool *arg_isnulls, int nargs) {
  FunctionCallInfo fcinfo;
  FmgrInfo flinfo;
  Datum result;
  int i;

  fcinfo = (FunctionCallInfo)palloc0(SizeForFunctionCallInfo(nargs));

  fmgr_info(functionId, &flinfo);
  InitFunctionCallInfoData(*fcinfo, &flinfo, 2, InvalidOid, NULL, NULL);

  for (i = 0; i < nargs; i++) {
    fcinfo->args[i].value = args[i];
    fcinfo->args[i].isnull = arg_isnulls[i];
  }

  result = FunctionCallInvoke(fcinfo);

  /* Check for null result, since caller is clearly not expecting one */
  if (fcinfo->isnull)
    elog(ERROR, "parquet_s3_fdw: function %u returned NULL", flinfo.fn_oid);

  return result;
}

static List *get_filenames_from_userfunc(const char *funcname,
                                         const char *funcarg) {
  Jsonb *j = NULL;
  Oid funcid;
  List *f = stringToQualifiedNameList(funcname);
  Datum filenames;
  Oid jsonboid = JSONBOID;
  Datum *values;
  bool *nulls;
  int num;
  List *res = NIL;
  ArrayType *arr;

  if (funcarg)
    j = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(funcarg)));

  funcid = LookupFuncName(f, 1, &jsonboid, false);
  filenames = OidFunctionCall1NullableArg(funcid, (Datum)j, funcarg == NULL);

  arr = DatumGetArrayTypeP(filenames);
  if (ARR_ELEMTYPE(arr) != TEXTOID)
    elog(ERROR,
         "parquet_s3_fdw: function returned an array with non-TEXT element "
         "type");

  deconstruct_array(arr, TEXTOID, -1, false, 'i', &values, &nulls, &num);

  if (num == 0) {
    elog(WARNING,
         "parquet_s3_fdw: '%s' function returned an empty array; foreign table "
         "wasn't created",
         get_func_name(funcid));
    return NIL;
  }

  for (int i = 0; i < num; ++i) {
    if (nulls[i])
      elog(ERROR,
           "parquet_s3_fdw: user function returned an array containing NULL "
           "value(s)");
    res = lappend(res, makeString(TextDatumGetCString(values[i])));
  }

  return res;
}

/*
 * get actual type for column in sorted option, coresponding type Oid list will
 * be returned.
 */
static void schemaless_get_sorted_column_type(Aws::S3::S3Client *s3_client,
                                              List *file_list, char *dirname,
                                              List *attrs_sorted,
                                              List **attrs_sorted_type) {
  ListCell *lc1, *lc2;
  int attrs_sorted_num = list_length(attrs_sorted);
  Oid *attrs_sorted_type_array = (Oid *)palloc(sizeof(Oid) * attrs_sorted_num);
  bool *attrs_sorted_is_taken = (bool *)palloc(sizeof(bool) * attrs_sorted_num);

  memset(attrs_sorted_is_taken, false, attrs_sorted_num);

  foreach (lc1, file_list) {
    std::unique_ptr<parquet::arrow::FileReader> reader;
    arrow::Status status;
    ReaderCacheEntry *reader_entry = NULL;
    std::string error;
    char *filename = strVal((Node *)lfirst(lc1));
    ;
    int attrs_sorted_idx = 0;

    /* Open parquet file to read meta information */
    try {
      if (s3_client) {
        char *dname;
        char *fname;
        parquetSplitS3Path(dirname, filename, &dname, &fname);
        reader_entry = parquetGetFileReader(s3_client, dname, fname);
        reader = std::move(reader_entry->file_reader->reader);
        pfree(dname);
        pfree(fname);
      } else {
        status = parquet::arrow::FileReader::Make(
            arrow::default_memory_pool(),
            parquet::ParquetFileReader::OpenFile(filename, false), &reader);
      }

      if (!status.ok())
        throw Error("parquet_impl schemaless_get_sorted_column_type: failed to open Parquet file %s",
                    status.message().c_str());

      auto meta = reader->parquet_reader()->metadata();
      parquet::ArrowReaderProperties props;
      parquet::arrow::SchemaManifest manifest;

      status = parquet::arrow::SchemaManifest::Make(meta->schema(), nullptr,
                                                    props, &manifest);
      if (!status.ok())
        throw Error("parquet_s3_fdw: error creating arrow schema");

      /*
       * Search for the column with the same name as sorted attribute
       */
      foreach (lc2, attrs_sorted) {
        char *attname = (char *)lfirst(lc2);

        for (auto &schema_field : manifest.schema_fields) {
          auto field_name = schema_field.field->name();
          char arrow_colname[NAMEDATALEN];

          if (field_name.length() > NAMEDATALEN - 1)
            throw Error("parquet column name '%s' is too long (max: %d)",
                        field_name.c_str(), NAMEDATALEN - 1);
          tolowercase(field_name.c_str(), arrow_colname);

          if (attrs_sorted_is_taken[attrs_sorted_idx] == false &&
              strcmp(attname, arrow_colname) == 0) {
            /* Found it! */
            auto arrow_type_id = schema_field.field->type()->id();
            attrs_sorted_is_taken[attrs_sorted_idx] = true;

            switch (arrow_type_id) {
              case arrow::Type::LIST:
              case arrow::Type::MAP:
                /* In schemaless mode, both NESTED LIST and MAP is mapping with
                 * JSONB  */
                attrs_sorted_type_array[attrs_sorted_idx] = JSONBOID;
                break;
              default:
                attrs_sorted_type_array[attrs_sorted_idx] =
                    to_postgres_type(arrow_type_id);
                break;
            }

            if (attrs_sorted_type_array[attrs_sorted_idx] == InvalidOid)
              elog(ERROR,
                   "parquet_s3_fdw: Can not get mapping type of '%s' column "
                   "from parquet file.",
                   attname);
            break;
          }
        } /* loop over parquet file columns */
        attrs_sorted_idx++;
      } /* loop over sorted columns */

      /* Get list type Oid from attrs_sorted_type_array */
      for (int i = list_length(*attrs_sorted_type); i < attrs_sorted_num; i++) {
        if (attrs_sorted_is_taken[i] == true) {
          *attrs_sorted_type =
              lappend_oid(*attrs_sorted_type, attrs_sorted_type_array[i]);
        } else {
          /* break to get missing sorted column from the next file */
          break;
        }
      }

      /* All sorted column type is taken */
      if (list_length(*attrs_sorted_type) == attrs_sorted_num) return;
    } catch (const std::exception &e) {
      error = e.what();
    }
    if (!error.empty()) {
      if (reader_entry) reader_entry->file_reader->reader = std::move(reader);
      elog(ERROR,
           "parquet_s3_fdw: failed to exctract column from Parquet file: %s",
           error.c_str());
    }
  } /* loop over list parquet file */

  elog(ERROR, "parquet_s3_fdw: '%s' column is not existed.",
       (char *)list_nth(attrs_sorted, list_length(*attrs_sorted_type)));
}

struct UsedColumnsContext {
  std::set<int> *cols;
  AttrNumber nattrs;
};

static Aws::S3::S3Client *ParquetGetConnectionByRelation(Relation relation) {
	static bool init_s3sdk = false;
	if (!init_s3sdk) {
		parquet_s3_init();
		init_s3sdk = true;
	}
  Aws::S3::S3Client *s3client = s3_client_open("minioadmin", "minioadmin", true, "127.0.0.1:9000", "ap-northeast-1");
  return s3client;
}

static bool UsedColumnsWalker(Node *node, struct UsedColumnsContext *ctx) {
  if (node == NULL) {
    return false;
  }

  if (IsA(node, Var)) {
    Var *var = (Var *)node;

    if (IS_SPECIAL_VARNO(var->varno)) {
      return false;
    }

    if (var->varattno > 0 && var->varattno <= ctx->nattrs) {
      ctx->cols->insert(var->varattno - 1);
    } else if (var->varattno == 0) {
      for (AttrNumber attno = 0; attno < ctx->nattrs; attno++) {
        ctx->cols->insert(attno);
      }
      return true;
    }
    return false;
  }

  return expression_tree_walker(node, (bool (*)())UsedColumnsWalker,
                                (void *)ctx);
}

static bool GetUsedColumns(Node *node, AttrNumber nattrs,
                           std::set<int> *cols_out) {
  struct UsedColumnsContext ctx;
  ctx.cols = cols_out;
  ctx.nattrs = nattrs;
  return UsedColumnsWalker(node, &ctx);
}

static ParquetScanDesc ParquetBeginRangeScanInternal(
    Relation relation, Snapshot snapshot,
    // Snapshot appendOnlyMetaDataSnapshot,
    std::list<std::string> filenames, int nkeys, ScanKey key,
    ParallelTableScanDesc parallel_scan, List *targetlist, List *qual,
    List *bitmapqualorig, uint32 flags, struct DynamicBitmapContext *bmCxt) {

  ParquetS3AccessState *state = NULL;
  ParquetScanDesc scan;

  std::set<int> attrs_used;
  std::vector<int> rowgroups;
  bool use_mmap = false;
  bool use_threads = false;
  bool schemaless = false;
  std::set<std::string> slcols;
  std::set<std::string> sorted_cols;
  std::list<SortSupportData> sort_keys;
  char *dirname = NULL;
  Aws::S3::S3Client *s3client = NULL;
  ReaderType reader_type = RT_MULTI;
  int max_open_files = 10;

  MemoryContextCallback *callback;
  MemoryContext reader_cxt;

  std::string error;

  scan = (ParquetScanDesc)palloc0(sizeof(ParquetScanDescData));
  scan->rs_base.rs_rd = relation;
  scan->rs_base.rs_snapshot = snapshot;
  scan->rs_base.rs_nkeys = nkeys;
  scan->rs_base.rs_flags = flags;
  scan->rs_base.rs_parallel = parallel_scan;

  TupleDesc tupDesc = RelationGetDescr(relation);
  GetUsedColumns((Node *)targetlist, tupDesc->natts, &attrs_used);

  s3client = ParquetGetConnectionByRelation(relation);

  TupleDesc tupleDesc = RelationGetDescr(relation);
  // TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

  reader_cxt = AllocSetContextCreate(NULL, "parquet_am tuple data",
                                     ALLOCSET_DEFAULT_SIZES);
  try {
    state = create_parquet_execution_state(
        reader_type, reader_cxt, dirname, s3client, tupleDesc, attrs_used,
        sort_keys, use_threads, use_mmap, max_open_files, schemaless, slcols,
        sorted_cols);

    for (auto it = filenames.begin(); it != filenames.end(); ++it) {
      state->add_file(it->data(), NULL);
    }
  } catch (std::exception &e) {
    error = e.what();
  }

  if (!error.empty()) elog(ERROR, "parquet_am: %s", error.c_str());

  /*
   * Enable automatic execution state destruction by using memory context
   * callback
   */
  callback = (MemoryContextCallback *)palloc(sizeof(MemoryContextCallback));
  callback->func = destroy_parquet_state;
  callback->arg = (void *)state;
  MemoryContextRegisterResetCallback(reader_cxt, callback);

  scan->state = state;
  return scan;
}

extern "C" TableScanDesc ParquetBeginScan(Relation relation, Snapshot snapshot,
                                          int nkeys, struct ScanKeyData *key,
                                          ParallelTableScanDesc pscan,
                                          uint32 flags) {
  ParquetScanDesc parquet_desc;

  /*
  seginfo = GetAllFileSegInfo(relation,
                                                          snapshot,
  &segfile_count, NULL);
                                                          */
  std::list<std::string> filenames;

  parquet_desc = ParquetBeginRangeScanInternal(relation, snapshot, filenames,
                                               // appendOnlyMetaDataSnapshot,
                                               // seginfo,
                                               // segfile_count,
                                               nkeys, key, pscan, NULL, NULL,
                                               NULL, flags, NULL);

  return (TableScanDesc)parquet_desc;
}

extern "C" bool ParquetGetNextSlot(TableScanDesc scan, ScanDirection direction,
                                   TupleTableSlot *slot) {
  ParquetScanDesc pscan = (ParquetScanDesc)scan;
  ParquetS3AccessState *festate = pscan->state;
  // TupleTableSlot             *slot = pscan->ss_ScanTupleSlot;
  std::string error;

  ExecClearTuple(slot);
  try {
    festate->next(slot);
  } catch (std::exception &e) {
    error = e.what();
  }
  if (!error.empty()) {
    elog(ERROR, "parquet_s3_fdw: %s", error.c_str());
    return false;
  }

  return true;
}

extern "C" void ParquetEndScan(TableScanDesc scan) {}

/*
 * find_cmp_func
 *      Find comparison function for two given types.
 */
static void find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2) {
  Oid cmp_proc_oid;
  TypeCacheEntry *tce_1, *tce_2;

  tce_1 = lookup_type_cache(type1, TYPECACHE_BTREE_OPFAMILY);
  tce_2 = lookup_type_cache(type2, TYPECACHE_BTREE_OPFAMILY);

  cmp_proc_oid = get_opfamily_proc(tce_1->btree_opf, tce_1->btree_opintype,
                                   tce_2->btree_opintype, BTORDER_PROC);
  fmgr_info(cmp_proc_oid, finfo);
}

extern "C" void
ParquetRescan(TableScanDesc scan, ScanKey key,
			  bool set_params, bool allow_strat,
			  bool allow_sync, bool allow_pagemode) {
  ParquetScanDesc pscan = (ParquetScanDesc)scan;
  ParquetS3AccessState *festate = pscan->state;
  festate->rescan();
}

// single table test

extern "C" void ParquetDmlInit(Relation rel) {
  // Oid    foreignTableId = InvalidOid;
  TupleDesc tupleDesc;
  MemoryContextCallback *callback;
  std::string error;
  arrow::Status status;
  std::set<std::string> sorted_cols;
  std::set<std::string> key_attrs;
  std::set<int> target_attrs;
  MemoryContext temp_cxt;

  bool use_threads = true;
  // ListCell lc;

  // Oid tableId = RelationGetRelid(rel);
  tupleDesc = RelationGetDescr(rel);

  std::vector<std::string> filenames;

  // TODO
  for (int i = 0; i < tupleDesc->natts; ++i) {
    target_attrs.insert(i);
  }

  char dirname[100];
  sprintf(dirname, "%d/%d/", rel->rd_node.dbNode, rel->rd_node.relNode);

  temp_cxt = AllocSetContextCreate(NULL, "parquet_s3_fdw temporary data",
                                   ALLOCSET_DEFAULT_SIZES);

  auto s3client = ParquetGetConnectionByRelation(rel);
  try {
    auto fmstate = CreateParquetModifyState(rel,
        temp_cxt, dirname, s3client, tupleDesc, use_threads);

    fmstate->set_rel_name(RelationGetRelationName(rel));
    for (size_t i = 0; i < filenames.size(); ++i) {
      fmstate->add_file(i, filenames[i].data());
    }

    // if (plstate->selector_function_name)
    //     fmstate->set_user_defined_func(plstate->selector_function_name);
  } catch (std::exception &e) {
    error = e.what();
  }
  if (!error.empty()) {
    elog(ERROR, "parquet_s3_fdw: %s", error.c_str());
  }

  /*
   * Enable automatic execution state destruction by using memory context
   * callback
   */
  callback = (MemoryContextCallback *)palloc(sizeof(MemoryContextCallback));
  // callback->func = destroy_parquet_modify_state;
  // callback->arg = //(void *)fmstate;
  // MemoryContextRegisterResetCallback(estate->es_query_cxt, callback);
}

extern "C" void ParquetDmlFinish(Relation rel) {
	auto fmstate = GetModifyState(rel);	
	if (fmstate != NULL) {
		return;
	}
    fmstate->upload();
  // ParquetS3FdwModifyState *fmstate = NULL;

  // Oid                     foreignTableId = InvalidOid;
  // foreignTableId = RelationGetRelid(rel);
}

extern "C" void ParquetInsert(Relation rel, HeapTuple* tuple,
                                   CommandId cid, int options,
                                   struct BulkInsertStateData *bistate, TransactionId xid) {
  std::string error;
  TupleTableSlot *slot;
  TupleDesc desc;
  desc = RelationGetDescr(rel);
  elog(INFO, "parquet insert finish: %s 1", error.c_str());
  slot = MakeTupleTableSlot(desc, &TTSOpsVirtual);
  //elog(ERROR, "parquet insert finish: %s 2", error.c_str());

   auto fmstate = GetModifyState(rel);	

   if (fmstate == nullptr) {
	  auto temp_cxt = AllocSetContextCreate(NULL, "parquet_s3_fdw temporary data",
									   ALLOCSET_DEFAULT_SIZES);
	  auto s3client = ParquetGetConnectionByRelation(rel);
	  fmstate = CreateParquetModifyState(rel,
			temp_cxt, "base/", s3client, desc, true);

		fmstate->set_rel_name(RelationGetRelationName(rel));
	
	}

  try {

	fmstate->exec_insert(slot);
  elog(PANIC, "parquet insert finish: %s 3", error.c_str());
    // if (plstate->selector_function_name)
    //     fmstate->set_user_defined_func(plstate->selector_function_name);
  } catch (std::exception &e) {
    error = e.what();
  }
  if (!error.empty()) {
    elog(ERROR, "parquet_s3_fdw: %s", error.c_str());
  }

  elog(ERROR, "parquet insert finish: %s", error.c_str());
  // return slot;
}



extern "C" void ParquetTupleInsert(Relation rel, TupleTableSlot *slot,
                                   CommandId cid, int options,
                                   struct BulkInsertStateData *bistate) {

  auto fmstate = GetModifyState(rel);	
  if (fmstate != NULL) {
		return;
	}
  fmstate->exec_insert(slot);
  // return slot;
}



/*
 *      Update one row
 */
extern "C" TM_Result 
ParquetTupleUpdate(Relation rel, ItemPointer otid,
                                        TupleTableSlot *slot, CommandId cid,
                                        Snapshot snapshot, Snapshot crosscheck,
                                        bool wait, TM_FailureData *tmfd,
                                        LockTupleMode *lockmode,
                                        bool *update_indexes) {

   auto fmstate = GetModifyState(rel);	
   if (fmstate != NULL) {
		return TM_Ok;
	}
  fmstate->exec_delete(otid);                              
  fmstate->exec_insert(slot);

  return TM_Ok;
}

extern "C" TM_Result 
ParquetTupleDelete(Relation relation, ItemPointer tid, CommandId cid,
				   Snapshot snapshot, Snapshot crosscheck, bool wait,
				   TM_FailureData *tmfd, bool changingPart) {

   auto fmstate = GetModifyState(relation);	
   if (fmstate != NULL) {
		return TM_Ok;
	}
	fmstate->exec_delete(tid);
	return TM_Ok;
}
