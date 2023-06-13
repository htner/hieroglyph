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

#include <sys/stat.h>
#include <math.h>
#include <list>
#include <set>

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/array.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/exception.h"
#include "parquet/file_reader.h"
#include "parquet/statistics.h"

#include "backend/access/parquet/parquet_s3/parquet_s3.hpp"
#include "backend/access/parquet/heap.hpp"
#include "backend/access/parquet/exec_state.hpp"
#include "backend/access/parquet/reader.hpp"
#include "backend/access/parquet/common.hpp"
#include "backend/access/parquet/slvars.hpp"
#include "backend/access/parquet/modify_reader.hpp"
#include "backend/access/parquet/modify_state.hpp"

extern "C"
{
#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/sysattr.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
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
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "funcapi.h"

#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#else
#include "access/table.h"
#include "access/relation.h"
#include "optimizer/optimizer.h"
#endif

#if PG_VERSION_NUM < 110000
#include "catalog/pg_am.h"
#else
#include "catalog/pg_am_d.h"
#endif
}


/* from costsize.c */
#define LOG2(x)  (log(x) / 0.693147180559945)

#if PG_VERSION_NUM < 110000
#define PG_GETARG_JSONB_P PG_GETARG_JSONB
#endif

#define IS_KEY_COLUMN(A)        ((strcmp(def->defname, "key") == 0) && \
                                 (defGetBoolean(def) == true))


bool enable_multifile;
bool enable_multifile_merge;


static void find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2);
static void destroy_parquet_state(void *arg);
static List *parse_attributes_list(char *start);
/*
 * Restriction
 */
struct ParquetScanDescData
{
	TableScanDescData rs_base;
	ParquetS3FdwExecutionState	*state;
};

typedef struct ParquetScanDescData* ParquetScanDesc;

struct RowGroupFilter
{
    AttrNumber  attnum;
    bool        is_key; /* for maps */
    Const      *value;
    int         strategy;
    char       *attname;    /* actual column name in schemales mode */
    Oid         atttype;   /* Explicit cast type in schemaless mode
                               In non-schemaless NULL is expectation  */
    bool        is_column;  /* for schemaless actual column `exist` operator */
};

/*
 * Plain C struct for fdw_state
 */
struct ParquetFdwPlanState
{
    List       *filenames;
    List       *attrs_sorted;
    Bitmapset  *attrs_used;     /* attributes actually used in query */
    bool        use_mmap;
    bool        use_threads;
    int32       max_open_files;
    bool        files_in_order;
    List       *rowgroups;      /* List of Lists (per filename) */
    uint64      matched_rows;
    ReaderType  type;
    char       *dirname;
    bool        schemaless;     /* In schemaless mode or not */
    schemaless_info slinfo;     /* Schemaless information */
    List       *slcols;         /* List actual column for schemaless mode */
    Aws::S3::S3Client *s3client;
    char       *selector_function_name;
    List       *key_columns;
};


static void parquet_s3_extract_slcols(ParquetFdwPlanState *fpinfo, PlannerInfo *root, RelOptInfo *baserel, List *tlist);

static int
get_strategy(Oid type, Oid opno, Oid am)
{
        Oid opclass;
    Oid opfamily;

    opclass = GetDefaultOpClass(type, am);

    if (!OidIsValid(opclass))
        return 0;

    opfamily = get_opclass_family(opclass);

    return get_op_opfamily_strategy(opno, opfamily);
}

static Const *
convert_const(Const *c, Oid dst_oid)
{
    Oid         funcid;
    CoercionPathType ct;

    ct = find_coercion_pathway(dst_oid, c->consttype,
                               COERCION_EXPLICIT, &funcid);
    switch (ct)
    {
        case COERCION_PATH_FUNC:
            {
                FmgrInfo    finfo;
                Const      *newc;
                int16       typlen;
                bool        typbyval;

                get_typlenbyval(dst_oid, &typlen, &typbyval);

                newc = makeConst(dst_oid,
                                 0,
                                 c->constcollid,
                                 typlen,
                                 0,
                                 c->constisnull,
                                 typbyval);
                fmgr_info(funcid, &finfo);
                newc->constvalue = FunctionCall1(&finfo, c->constvalue);

                return newc;
            }
        case COERCION_PATH_RELABELTYPE:
            /* Cast is not needed */
            break;
        case COERCION_PATH_COERCEVIAIO:
            {
                /*
                 * In this type of cast we need to output the value to a string
                 * and then feed this string to the input function of the
                 * target type.
                 */
                Const  *newc;
                int16   typlen;
                bool    typbyval;
                Oid     input_fn, output_fn;
                Oid     input_param;
                bool    isvarlena;
                // char   *str; FIXME

                /* Construct a new Const node */
                get_typlenbyval(dst_oid, &typlen, &typbyval);
                newc = makeConst(dst_oid,
                                 0,
                                 c->constcollid,
                                 typlen,
                                 0,
                                 c->constisnull,
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
static bool
row_group_matches_filter(parquet::Statistics *stats,
                         const arrow::DataType *arrow_type,
                         RowGroupFilter *filter)
{
    FmgrInfo finfo;
    Datum    val;
    int      collid = filter->value->constcollid;
    int      strategy = filter->strategy;

    if (arrow_type->id() == arrow::Type::MAP && filter->is_key)
    {
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
        filter->value = convert_const(filter->value,
                                      to_postgres_type(arrow_type->id()));
    }
    val = filter->value->constvalue;

    find_cmp_func(&finfo,
                  filter->value->consttype,
                  to_postgres_type(arrow_type->id()));

    switch (filter->strategy)
    {
        case BTLessStrategyNumber:
        case BTLessEqualStrategyNumber:
            {
                Datum   lower;
                int     cmpres;
                bool    satisfies;
                std::string min = std::move(stats->EncodeMin());

                lower = bytes_to_postgres_type(min.c_str(), min.length(),
                                               arrow_type);
                cmpres = FunctionCall2Coll(&finfo, collid, val, lower);

                satisfies =
                    (strategy == BTLessStrategyNumber      && cmpres > 0) ||
                    (strategy == BTLessEqualStrategyNumber && cmpres >= 0);

                if (!satisfies)
                    return false;
                break;
            }

        case BTGreaterStrategyNumber:
        case BTGreaterEqualStrategyNumber:
            {
                Datum   upper;
                int     cmpres;
                bool    satisfies;
                std::string max = std::move(stats->EncodeMax());

                upper = bytes_to_postgres_type(max.c_str(), max.length(),
                                               arrow_type);
                cmpres = FunctionCall2Coll(&finfo, collid, val, upper);

                satisfies =
                    (strategy == BTGreaterStrategyNumber      && cmpres < 0) ||
                    (strategy == BTGreaterEqualStrategyNumber && cmpres <= 0);

                if (!satisfies)
                    return false;
                break;
            }

        case BTEqualStrategyNumber:
        case JsonbExistsStrategyNumber:
            {
                Datum   lower,
                        upper;
                std::string min = std::move(stats->EncodeMin());
                std::string max = std::move(stats->EncodeMax());

                lower = bytes_to_postgres_type(min.c_str(), min.length(),
                                               arrow_type);
                upper = bytes_to_postgres_type(max.c_str(), max.length(),
                                               arrow_type);

                int l = FunctionCall2Coll(&finfo, collid, val, lower);
                int u = FunctionCall2Coll(&finfo, collid, val, upper);

                if (l < 0 || u > 0)
                    return false;
                break;
            }

        default:
            /* should not happen */
            Assert(false);
    }

    return true;
}

typedef enum
{
    PS_START = 0,
    PS_IDENT,
    PS_QUOTE
} ParserState;

/*
 * parse_filenames_list
 *      Parse space separated list of filenames.
 */
static List *
parse_filenames_list(const char *str)
{
    char       *cur = pstrdup(str);
    char       *f = cur;
    ParserState state = PS_START;
    List       *filenames = NIL;
    FileLocation loc = LOC_NOT_DEFINED;

    while (*cur)
    {
        switch (state)
        {
            case PS_START:
                switch (*cur)
                {
                    case ' ':
                        /* just skip */
                        break;
                    case '"':
                        f = cur + 1;
                        state = PS_QUOTE;
                        break;
                    default:
                        /* XXX we should check that *cur is a valid path symbol
                         * but let's skip it for now */
                        state = PS_IDENT;
                        f = cur;
                        break;
                }
                break;
            case PS_IDENT:
                switch (*cur)
                {
                    case ' ':
                        *cur = '\0';
                        loc = parquetFilenamesValidator(f, loc);
                        filenames = lappend(filenames, makeString(f));
                        state = PS_START;
                        f = NULL;
                        break;
                    default:
                        break;
                }
                break;
            case PS_QUOTE:
                switch (*cur)
                {
                    case '"':
                        *cur = '\0';
                        loc = parquetFilenamesValidator(f, loc);
                        filenames = lappend(filenames, makeString(f));
                        state = PS_START;
                        f = NULL;
                        break;
                    default:
                        break;
                }
                break;
            default:
                elog(ERROR, "parquet_s3_fdw: unknown parse state");
        }
        cur++;
    }
    if (f != NULL)
    {
        loc = parquetFilenamesValidator(f, loc);
        filenames = lappend(filenames, makeString(f));
    }

    return filenames;
}

static bool
parquet_s3_column_is_existed(parquet::arrow::SchemaManifest manifest, char *column_name)
{
    for (auto &schema_field : manifest.schema_fields)
    {
        auto       &field = schema_field.field;
        char        arrow_colname[NAMEDATALEN];

        if (field->name().length() > NAMEDATALEN - 1)
            throw Error("parquet column name '%s' is too long (max: %d)",
                        field->name().c_str(), NAMEDATALEN - 1);
        tolowercase(field->name().c_str(), arrow_colname);

        if (strcmp(column_name, arrow_colname) == 0)
            return true;    /* Found!!! */
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
List *
extract_rowgroups_list(const char *filename,
                       const char *dirname,
                       Aws::S3::S3Client *s3_client,
                       TupleDesc tupleDesc,
                       std::list<RowGroupFilter> &filters,
                       uint64 *matched_rows,
                       uint64 *total_rows,
                       bool schemaless) noexcept
{
    std::unique_ptr<parquet::arrow::FileReader> reader;
    arrow::Status   status;
    List           *rowgroups = NIL;
    ReaderCacheEntry *reader_entry  = NULL;
    std::string     error;

    /* Open parquet file to read meta information */
    try
    {
        if (s3_client)
        {
            char *dname;
            char *fname;
            parquetSplitS3Path(dirname, filename, &dname, &fname);
            reader_entry = parquetGetFileReader(s3_client, dname, fname);
            reader = std::move(reader_entry->file_reader->reader);
            pfree(dname);
            pfree(fname);
        }
        else
        {
            status = parquet::arrow::FileReader::Make(
                    arrow::default_memory_pool(),
                    parquet::ParquetFileReader::OpenFile(filename, false),
                    &reader);
        }

        if (!status.ok())
            throw Error("parquet_s3_fdw: failed to open Parquet file %s", status.message().c_str());

        auto meta = reader->parquet_reader()->metadata();
        parquet::ArrowReaderProperties  props;
        parquet::arrow::SchemaManifest  manifest;

        status = parquet::arrow::SchemaManifest::Make(meta->schema(), nullptr,
                                                      props, &manifest);
        if (!status.ok())
            throw Error("parquet_s3_fdw: error creating arrow schema");

        /* Check each row group whether it matches the filters */
        for (int r = 0; r < reader->num_row_groups(); r++)
        {
            bool match = true;
            auto rowgroup = meta->RowGroup(r);

            /* Skip empty rowgroups */
            if (!rowgroup->num_rows())
                continue;

            for (auto &filter : filters)
            {
                AttrNumber      attnum;
                char            pg_colname[NAMEDATALEN];

                if (schemaless)
                {
                    /* In schemaless mode, attname has already existed  */
                    tolowercase(filter.attname, pg_colname);

                    if (filter.is_column == true)
                    {
                        /*
                         * Check column existed for condition: v ? column
                         * If column is not existed, exclude current file from file list.
                         */
                        if ((match = parquet_s3_column_is_existed(manifest, pg_colname)) == false)
                        {
                            elog(DEBUG1, "parquet_s3_fdw: skip file %s", filename);
                            return NIL;
                        }
                        continue;
                    }
                }
                else
                {
                    attnum = filter.attnum - 1;
                    tolowercase(NameStr(TupleDescAttr(tupleDesc, attnum)->attname),
                                pg_colname);
                }

                /*
                 * Search for the column with the same name as filtered attribute
                 */
                for (auto &schema_field : manifest.schema_fields)
                {
                    MemoryContext   ccxt = CurrentMemoryContext;
                    bool            error = false;
                    char            errstr[ERROR_STR_LEN];
                    char            arrow_colname[NAMEDATALEN];
                    auto           &field = schema_field.field;
                    int             column_index;

                    /* Skip complex objects (lists, structs except maps) */
                    if (schema_field.column_index == -1
                        && field->type()->id() != arrow::Type::MAP)
                        continue;

                    if (field->name().length() > NAMEDATALEN - 1)
                        throw Error("parquet column name '%s' is too long (max: %d)",
                                    field->name().c_str(), NAMEDATALEN - 1);
                    tolowercase(field->name().c_str(), arrow_colname);

                    if (strcmp(pg_colname, arrow_colname) != 0)
                        continue;

                    /* in schemaless mode, skip filter if parquet column type is not match with actual column (explicit cast) type */
                    if (schemaless)
                    {
                        int arrow_type = field->type().get()->id();

                        if (!(filter.atttype == to_postgres_type(arrow_type) ||
                              (filter.atttype == JSONBOID &&
                               arrow_type == arrow::Type::MAP)))
                            continue;
                    }

                    if (field->type()->id() == arrow::Type::MAP)
                    {
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
                    }
                    else
                        column_index = schema_field.column_index;

                    /* Found it! */
                    std::shared_ptr<parquet::Statistics>  stats;
                    auto column = rowgroup->ColumnChunk(column_index);
                    stats = column->statistics();

                    PG_TRY();
                    {
                        /*
                         * If at least one filter doesn't match rowgroup exclude
                         * the current row group and proceed with the next one.
                         */
                        if (stats && !row_group_matches_filter(stats.get(),
                                                               field->type().get(),
                                                               &filter))
                        {
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
                        throw Error("parquet_s3_fdw: row group filter match failed: %s", errstr);
                    break;
                }  /* loop over columns */

                if (!match)
                    break;

            }  /* loop over filters */

            /* All the filters match this rowgroup */
            if (match)
            {
                /* TODO: PG_TRY */
                rowgroups = lappend_int(rowgroups, r);
                *matched_rows += rowgroup->num_rows();
            }
            *total_rows += rowgroup->num_rows();
        }  /* loop over rowgroups */
    }
    catch(const std::exception& e) {
        error = e.what();      
    }
    if (!error.empty()) {
        if (reader_entry)
            reader_entry->file_reader->reader = std::move(reader);
        elog(ERROR,
             "parquet_s3_fdw: failed to exctract row groups from Parquet file: %s",
             error.c_str());
    }

    return rowgroups;
}

struct FieldInfo
{
    char    name[NAMEDATALEN];
    Oid     oid;
};

/*
 * extract_parquet_fields
 *      Read parquet file and return a list of its fields
 */
List *
extract_parquet_fields(const char *path, const char *dirname, Aws::S3::S3Client *s3_client) noexcept
{
    List           *res = NIL;
    std::string     error;

    try
    {
        std::unique_ptr<parquet::arrow::FileReader> reader;
        parquet::ArrowReaderProperties props;
        parquet::arrow::SchemaManifest manifest;
        arrow::Status   status;
        FieldInfo      *fields;

        if (s3_client)
        {
            arrow::MemoryPool* pool = arrow::default_memory_pool();
            char *dname;
            char *fname;
            parquetSplitS3Path(dirname, path, &dname, &fname);
            std::shared_ptr<arrow::io::RandomAccessFile> input(new S3RandomAccessFile(s3_client, dname, fname));
            status = parquet::arrow::OpenFile(input, pool, &reader);
            pfree(dname);
            pfree(fname);
        }
        else
        {
            status = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(path, false),
                        &reader);
        }
        if (!status.ok())
            throw Error("parquet_s3_fdw: failed to open Parquet file %s",
                                 status.message().c_str());

        auto p_schema = reader->parquet_reader()->metadata()->schema();
        if (!parquet::arrow::SchemaManifest::Make(p_schema, nullptr, props, &manifest).ok())
            throw std::runtime_error("parquet_s3_fdw: error creating arrow schema");

        fields = (FieldInfo *) exc_palloc(
                sizeof(FieldInfo) * manifest.schema_fields.size());

        for (auto &schema_field : manifest.schema_fields)
        {
            auto   &field = schema_field.field;
            auto   &type = field->type();
            Oid     pg_type;

            switch (type->id())
            {
                case arrow::Type::LIST:
                {
                    arrow::Type::type subtype_id;
                    Oid     pg_subtype;
                    bool    error = false;

                    if (type->num_fields() != 1)
                        throw std::runtime_error("lists of structs are not supported");

                    subtype_id = get_arrow_list_elem_type(type.get());
                    pg_subtype = to_postgres_type(subtype_id);

                    /* This sucks I know... */
                    PG_TRY();
                    {
                        pg_type = get_array_type(pg_subtype);
                    }
                    PG_CATCH();
                    {
                        error = true;
                    }
                    PG_END_TRY();

                    if (error)
                        throw std::runtime_error("failed to get the type of array elements");
                    break;
                }
                case arrow::Type::MAP:
                    pg_type = JSONBOID;
                    break;
                default:
                    pg_type = to_postgres_type(type->id());
            }

            if (pg_type != InvalidOid)
            {
                if (field->name().length() > 63)
                    throw Error("parquet_s3_fdw: field name '%s' in '%s' is too long",
                                field->name().c_str(), path);

                memcpy(fields->name, field->name().c_str(), field->name().length() + 1);
                fields->oid = pg_type;
                res = lappend(res, fields++);
            }
            else
            {
                throw Error("parquet_s3_fdw: cannot convert field '%s' of type '%s' in %s",
                            field->name().c_str(), type->name().c_str(), path);
            }
        }
    }
    catch (std::exception &e)
    {
        error = e.what();
    }
    if (!error.empty())
        elog(ERROR, "parquet_s3_fdw: %s", error.c_str());

    return res;
}

/*
 * create_foreign_table_query
 *      Produce a query text for creating a new foreign table.
 */
char *
create_foreign_table_query(const char *tablename,
                           const char *schemaname,
                           const char *servername,
                           char **paths, int npaths,
                           List *fields, List *options)
{
    StringInfoData  str;
    ListCell       *lc;
    bool		    schemaless = false;
    bool            is_first = true;
    List           *key_columns = NIL;

    /* list options */
    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "schemaless") == 0)
            schemaless = defGetBoolean(def);
        else if (strcmp(def->defname, "key_columns") == 0)
            key_columns = parse_attributes_list(pstrdup(defGetString(def)));
    }

    initStringInfo(&str);
    appendStringInfo(&str, "CREATE FOREIGN TABLE ");

    /* append table name */
    if (schemaname)
        appendStringInfo(&str, "%s.%s (",
                         quote_identifier(schemaname), quote_identifier(tablename));
    else
        appendStringInfo(&str, "%s (", quote_identifier(tablename));

    /* append columns */
    if (schemaless == true)
    {
        /* for schemaless mode, columns specify always 'v jsonb' */
        appendStringInfoString(&str, "v jsonb");
    }
    else
    {
        foreach (lc, fields)
        {
            FieldInfo  *field = (FieldInfo *) lfirst(lc);
            char       *name = field->name;
            Oid         pg_type = field->oid;
            const char *type_name = format_type_be(pg_type);

            if (!is_first)
                appendStringInfo(&str, ", %s %s", quote_identifier(name), type_name);
            else
            {
                appendStringInfo(&str, "%s %s", quote_identifier(name), type_name);
                is_first = false;
            }

            if (key_columns)
            {
                ListCell *lc2;

                foreach (lc2, key_columns)
                {
                    char   *key_column_name = (char *) lfirst(lc2);

                    if (strcmp(name, key_column_name) == 0)
                    {
                        appendStringInfo(&str, " OPTIONS (key 'true')");
                        break;
                    }
                }
            }
        }
    }

    appendStringInfo(&str, ") SERVER %s ", quote_identifier(servername));
    appendStringInfo(&str, "OPTIONS (filename '");

    /* list paths */
    is_first = true;
    for (int i = 0; i < npaths; ++i)
    {
        if (!is_first)
            appendStringInfoChar(&str, ' ');
        else
            is_first = false;

        appendStringInfoString(&str, paths[i]);
    }
    appendStringInfoChar(&str, '\'');

    /* list options */
    foreach(lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        /* ignore key_columns in non schemaless mode */
        if (!schemaless && strcmp(def->defname, "key_columns") == 0)
            continue;

        appendStringInfo(&str, ", %s '%s'", def->defname, defGetString(def));
    }

    appendStringInfo(&str, ")");

    return str.data;
}

static void
destroy_parquet_state(void *arg)
{
    ParquetS3FdwExecutionState *festate = (ParquetS3FdwExecutionState *) arg;

    if (festate)
        delete festate;
}

static void
destroy_parquet_modify_state(void *arg)
{
    ParquetS3FdwModifyState *fmstate = (ParquetS3FdwModifyState *) arg;

    if (fmstate && fmstate->has_s3_client())
    {
        /*
         * After modify, parquet file information on S3 server is different with cached one,
         * so, disable connection imediately after modify to reload this infomation.
         */
        parquet_disconnect_s3_server();
        delete fmstate;
    }

}

/*
 * C interface functions
 */

static List *
parse_attributes_list(char *start)
{
    List      *attrs = NIL;
    char      *token;
    const char *delim = " ";

    while ((token = strtok(start, delim)) != NULL)
    {
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
static Datum
OidFunctionCall1NullableArg(Oid functionId, Datum arg, bool argisnull)
{
#if PG_VERSION_NUM < 120000
    FunctionCallInfoData    _fcinfo;
    FunctionCallInfoData    *fcinfo = &_fcinfo;
#else
	LOCAL_FCINFO(fcinfo, 1);
#endif
    FmgrInfo    flinfo;
    Datum		result;

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
static Datum
OidFunctionCallnNullableArg(Oid functionId, Datum *args, bool *arg_isnulls, int nargs)
{
	FunctionCallInfo fcinfo;
    FmgrInfo    flinfo;
    Datum		result;
    int         i;

    fcinfo = (FunctionCallInfo) palloc0(SizeForFunctionCallInfo(nargs));

    fmgr_info(functionId, &flinfo);
    InitFunctionCallInfoData(*fcinfo, &flinfo, 2, InvalidOid, NULL, NULL);

    for (i = 0; i < nargs; i++)
    {
        fcinfo->args[i].value = args[i];
        fcinfo->args[i].isnull = arg_isnulls[i];
    }

    result = FunctionCallInvoke(fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo->isnull)
        elog(ERROR, "parquet_s3_fdw: function %u returned NULL", flinfo.fn_oid);

    return result;
}

static List *
get_filenames_from_userfunc(const char *funcname, const char *funcarg)
{
    Jsonb      *j = NULL;
    Oid         funcid;
    List       *f = stringToQualifiedNameList(funcname);
    Datum       filenames;
    Oid         jsonboid = JSONBOID;
    Datum      *values;
    bool       *nulls;
    int         num;
    List       *res = NIL;
    ArrayType  *arr;

    if (funcarg)
        j = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(funcarg)));

    funcid = LookupFuncName(f, 1, &jsonboid, false);
    filenames = OidFunctionCall1NullableArg(funcid, (Datum) j, funcarg == NULL);

    arr = DatumGetArrayTypeP(filenames);
    if (ARR_ELEMTYPE(arr) != TEXTOID)
        elog(ERROR, "parquet_s3_fdw: function returned an array with non-TEXT element type");

    deconstruct_array(arr, TEXTOID, -1, false, 'i', &values, &nulls, &num);

    if (num == 0)
    {
        elog(WARNING,
             "parquet_s3_fdw: '%s' function returned an empty array; foreign table wasn't created",
             get_func_name(funcid));
        return NIL;
    }

    for (int i = 0; i < num; ++i)
    {
        if (nulls[i])
            elog(ERROR, "parquet_s3_fdw: user function returned an array containing NULL value(s)");
        res = lappend(res, makeString(TextDatumGetCString(values[i])));
    }

    return res;
}

static void
get_table_options(Oid relid, ParquetFdwPlanState *fdw_private)
{
    ForeignTable *table;
    ListCell     *lc;
    char         *funcname = NULL;
    char         *funcarg = NULL;

    fdw_private->use_mmap = false;
    fdw_private->use_threads = false;
    fdw_private->max_open_files = 0;
    fdw_private->files_in_order = false;
    fdw_private->schemaless = false;
    fdw_private->key_columns = NIL;
    table = GetForeignTable(relid);

    foreach(lc, table->options)
    {
		DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            fdw_private->filenames = parse_filenames_list(defGetString(def));
        }
        else if (strcmp(def->defname, "files_func") == 0)
        {
            funcname = defGetString(def);
        }
        else if (strcmp(def->defname, "files_func_arg") == 0)
        {
            funcarg = defGetString(def);
        }
        else if (strcmp(def->defname, "sorted") == 0)
        {
            fdw_private->attrs_sorted =
                parse_attributes_list(defGetString(def));
        }
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            fdw_private->use_mmap = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "use_threads") == 0)
        {
            fdw_private->use_threads = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "dirname") == 0)
        {
            fdw_private->dirname = defGetString(def);
        }
        else if (strcmp(def->defname, "max_open_files") == 0)
        {
            /* check that int value is valid */
#if PG_VERSION_NUM >= 150000
            fdw_private->max_open_files = pg_strtoint32(defGetString(def));
#else
            fdw_private->max_open_files = pg_atoi(defGetString(def), sizeof(int32), '\0');
#endif
        }
        else if (strcmp(def->defname, "files_in_order") == 0)
        {
            fdw_private->files_in_order = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "schemaless") == 0)
        {
            fdw_private->schemaless = defGetBoolean(def);
        }
        else if (strcmp(def->defname, "insert_file_selector") == 0)
        {
            fdw_private->selector_function_name = defGetString(def);
        }
        else if (strcmp(def->defname, "key_columns") == 0)
        {
            fdw_private->key_columns = parse_attributes_list(defGetString(def));
        }
        else
            elog(ERROR, "parquet_s3_fdw: unknown option '%s'", def->defname);
    }

    if (funcname)
        fdw_private->filenames = get_filenames_from_userfunc(funcname, funcarg);
}

/*
 * get actual type for column in sorted option, coresponding type Oid list will be returned.
 */
static void
schemaless_get_sorted_column_type(Aws::S3::S3Client *s3_client, List *file_list, char *dirname, List *attrs_sorted, List **attrs_sorted_type)
{
    ListCell   *lc1, *lc2;
    int         attrs_sorted_num = list_length(attrs_sorted);
    Oid        *attrs_sorted_type_array = (Oid *)palloc(sizeof(Oid) * attrs_sorted_num);
    bool       *attrs_sorted_is_taken = (bool *)palloc(sizeof(bool) * attrs_sorted_num);

    memset(attrs_sorted_is_taken, false, attrs_sorted_num);

    foreach(lc1, file_list)
    {
        std::unique_ptr<parquet::arrow::FileReader> reader;
        arrow::Status   status;
        ReaderCacheEntry *reader_entry  = NULL;
        std::string     error;
        char           *filename = strVal((Node *) lfirst(lc1));;
        int             attrs_sorted_idx = 0;

        /* Open parquet file to read meta information */
        try
        {
            if (s3_client)
            {
                char *dname;
                char *fname;
                parquetSplitS3Path(dirname, filename, &dname, &fname);
                reader_entry = parquetGetFileReader(s3_client, dname, fname);
                reader = std::move(reader_entry->file_reader->reader);
                pfree(dname);
                pfree(fname);
            }
            else
            {
                status = parquet::arrow::FileReader::Make(
                        arrow::default_memory_pool(),
                        parquet::ParquetFileReader::OpenFile(filename, false),
                        &reader);
            }

            if (!status.ok())
                throw Error("parquet_s3_fdw: failed to open Parquet file %s", status.message().c_str());

            auto meta = reader->parquet_reader()->metadata();
            parquet::ArrowReaderProperties  props;
            parquet::arrow::SchemaManifest  manifest;

            status = parquet::arrow::SchemaManifest::Make(meta->schema(), nullptr,
                                                        props, &manifest);
            if (!status.ok())
                throw Error("parquet_s3_fdw: error creating arrow schema");

            /*
             * Search for the column with the same name as sorted attribute
             */
            foreach(lc2, attrs_sorted)
            {
                char *attname = (char *) lfirst(lc2);

                for (auto &schema_field : manifest.schema_fields)
                {
                    auto field_name = schema_field.field->name();
                    char arrow_colname[NAMEDATALEN];

                    if (field_name.length() > NAMEDATALEN - 1)
                        throw Error("parquet column name '%s' is too long (max: %d)",
                                    field_name.c_str(), NAMEDATALEN - 1);
                    tolowercase(field_name.c_str(), arrow_colname);

                    if (attrs_sorted_is_taken[attrs_sorted_idx] == false && strcmp(attname, arrow_colname) == 0)
                    {
                        /* Found it! */
                        auto arrow_type_id = schema_field.field->type()->id();
                        attrs_sorted_is_taken[attrs_sorted_idx] = true;

                        switch (arrow_type_id)
                        {
                            case arrow::Type::LIST:
                            case arrow::Type::MAP:
                                /* In schemaless mode, both NESTED LIST and MAP is mapping with JSONB  */
                                attrs_sorted_type_array[attrs_sorted_idx] = JSONBOID;
                                break;
                            default:
                                attrs_sorted_type_array[attrs_sorted_idx] = to_postgres_type(arrow_type_id);
                                break;
                        }

                        if (attrs_sorted_type_array[attrs_sorted_idx] == InvalidOid)
                            elog(ERROR, "parquet_s3_fdw: Can not get mapping type of '%s' column from parquet file.", attname);
                        break;
                    }
                }   /* loop over parquet file columns */
                attrs_sorted_idx++;
            }  /* loop over sorted columns */

            /* Get list type Oid from attrs_sorted_type_array */
            for (int i = list_length(*attrs_sorted_type); i < attrs_sorted_num; i++)
            {
                if (attrs_sorted_is_taken[i] == true)
                {
                    *attrs_sorted_type = lappend_oid(*attrs_sorted_type, attrs_sorted_type_array[i]);
                }
                else
                {
                    /* break to get missing sorted column from the next file */
                    break;
                }
            }

            /* All sorted column type is taken */
            if (list_length(*attrs_sorted_type) == attrs_sorted_num)
                return;
        }
        catch(const std::exception& e) {
            error = e.what();
        }
        if (!error.empty()) {
            if (reader_entry)
                reader_entry->file_reader->reader = std::move(reader);
            elog(ERROR,
                "parquet_s3_fdw: failed to exctract column from Parquet file: %s",
                error.c_str());
        }
    }   /* loop over list parquet file */

    elog(ERROR, "parquet_s3_fdw: '%s' column is not existed.", (char *) list_nth(attrs_sorted, list_length(*attrs_sorted_type)));
}

struct UsedColumnsContext {
	std::set<int>* cols;
	AttrNumber nattrs;
};

static Aws::S3::S3Client*
ParquetGetConnectionByRelation(Relation relation) {
	Aws::S3::S3Client *s3client = NULL;
	return s3client;
}

static bool
UsedColumnsWalker(Node *node, struct UsedColumnsContext *ctx)
{
	if (node == NULL) {
		return false;
	}

	if (IsA(node, Var)) {
		Var *var = (Var *) node;

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

	return expression_tree_walker(node, (bool (*)())UsedColumnsWalker, (void *)ctx);
}

static bool
GetUsedColumns(Node* node, AttrNumber nattrs, std::set<int>* cols_out) {
	struct UsedColumnsContext ctx;
	ctx.cols = cols_out;
	ctx.nattrs = nattrs;
	return UsedColumnsWalker(node, &ctx);
}

static ParquetScanDesc
ParquetBeginRangeScanInternal(Relation relation,
								   Snapshot snapshot,
								   //Snapshot appendOnlyMetaDataSnapshot,
								   std::list<std::string> filenames, 
								   int nkeys,
								   ScanKey key,
								   ParallelTableScanDesc parallel_scan,
								   List *targetlist,
								   List *qual,
								   List *bitmapqualorig,
								   uint32 flags,
								   struct DynamicBitmapContext *bmCxt)
{
    ParquetS3FdwExecutionState   *state = NULL;
	ParquetScanDesc scan;

	std::set<int>   attrs_used;
	std::vector<int>	rowgroups;
    bool            use_mmap = false;
    bool            use_threads = false;
    bool            schemaless = false;
    std::set<std::string> slcols;
    std::set<std::string> sorted_cols;
    std::list<SortSupportData> sort_keys;
    char           *dirname = NULL;
    Aws::S3::S3Client *s3client = NULL;
    ReaderType      reader_type = RT_MULTI;
    int             max_open_files = 10;

	MemoryContextCallback      *callback;
    MemoryContext   reader_cxt;

    std::string     error;


	scan = (ParquetScanDesc) palloc0(sizeof(ParquetScanDescData));
	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

    TupleDesc tupDesc = RelationGetDescr(relation);
	GetUsedColumns((Node *)targetlist, tupDesc->natts, &attrs_used);

    s3client = ParquetGetConnectionByRelation(relation);

    TupleDesc       tupleDesc = RelationGetDescr(relation);
    // TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

    reader_cxt = AllocSetContextCreate(NULL,
                                       "parquet_am tuple data",
                                       ALLOCSET_DEFAULT_SIZES);
    try
    {
        state = create_parquet_execution_state(reader_type, reader_cxt, dirname, s3client, tupleDesc,
                                                 attrs_used, sort_keys,
                                                 use_threads, use_mmap,
                                                 max_open_files, schemaless,
                                                 slcols, sorted_cols);

		for (auto it = filenames.begin(); it != filenames.end(); ++it) {
			state->add_file(it->data(), NULL);
		}
    } catch(std::exception &e) {
        error = e.what();
    }

    if (!error.empty())
        elog(ERROR, "parquet_am: %s", error.c_str());

    /*
     * Enable automatic execution state destruction by using memory context
     * callback
     */
    callback = (MemoryContextCallback *) palloc(sizeof(MemoryContextCallback));
    callback->func = destroy_parquet_state;
    callback->arg = (void *) state;
    MemoryContextRegisterResetCallback(reader_cxt, callback);

    scan->state = state;
	return scan;
}


extern "C" TableScanDesc 
ParquetBeginScan(Relation relation,
				 Snapshot snapshot,
				 int nkeys, struct ScanKeyData *key,
				 ParallelTableScanDesc pscan,
				 uint32 flags)
{
	ParquetScanDesc parquet_desc;

	/*
	seginfo = GetAllFileSegInfo(relation,
								snapshot, &segfile_count, NULL);
								*/
	std::list<std::string> filenames;

	parquet_desc = ParquetBeginRangeScanInternal(relation,
												snapshot,
												filenames,
												// appendOnlyMetaDataSnapshot,
												//seginfo,
												//segfile_count,
												nkeys,
												key,
												pscan,
												NULL,
												NULL,
												NULL,
												flags,
												NULL);

	return (TableScanDesc) parquet_desc;
}

extern "C" bool		
ParquetGetNextSlot(TableScanDesc scan,
					ScanDirection direction,
					TupleTableSlot *slot) {
	ParquetScanDesc pscan = (ParquetScanDesc)scan; 
    ParquetS3FdwExecutionState   *festate = pscan->state;
    //TupleTableSlot             *slot = pscan->ss_ScanTupleSlot;
    std::string                 error;

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

extern "C" void
ParquetEndScan(TableScanDesc scan) {

}

/*
 * find_cmp_func
 *      Find comparison function for two given types.
 */
static void
find_cmp_func(FmgrInfo *finfo, Oid type1, Oid type2)
{
    Oid cmp_proc_oid;
    TypeCacheEntry *tce_1, *tce_2;

    tce_1 = lookup_type_cache(type1, TYPECACHE_BTREE_OPFAMILY);
    tce_2 = lookup_type_cache(type2, TYPECACHE_BTREE_OPFAMILY);

    cmp_proc_oid = get_opfamily_proc(tce_1->btree_opf,
                                     tce_1->btree_opintype,
                                     tce_2->btree_opintype,
                                     BTORDER_PROC);
    fmgr_info(cmp_proc_oid, finfo);
}

extern "C" TupleTableSlot *
parquetIterateForeignScan(ForeignScanState *node)
{
    ParquetS3FdwExecutionState   *festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    TupleTableSlot             *slot = node->ss.ss_ScanTupleSlot;
    std::string                 error;

    ExecClearTuple(slot);
    try
    {
        festate->next(slot);
    }
    catch (std::exception &e)
    {
        error = e.what();
    }
    if (!error.empty())
        elog(ERROR, "parquet_s3_fdw: %s", error.c_str());

    return slot;
}

extern "C" void
parquetReScanForeignScan(ForeignScanState *node)
{
    ParquetS3FdwExecutionState   *festate = (ParquetS3FdwExecutionState *) node->fdw_state;

    festate->rescan();
}

/*
 * parquetExplainForeignScan
 *      Additional explain information, namely row groups list.
 */


/* Parallel query execution */

extern "C" bool
parquetIsForeignScanParallelSafe(PlannerInfo * /* root */,
                                 RelOptInfo *rel,
                                 RangeTblEntry * /* rte */)
{
    /* Plan nodes that reference a correlated SubPlan is always parallel restricted. 
     * Therefore, return false when there is lateral join.
     */
    if (rel->lateral_relids)
        return false;
    return true;
}

extern "C" Size
parquetEstimateDSMForeignScan(ForeignScanState *node,
                              ParallelContext * /* pcxt */)
{
    ParquetS3FdwExecutionState   *festate;

    festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    return festate->estimate_coord_size();
}

extern "C" void
parquetInitializeDSMForeignScan(ForeignScanState *node,
                                ParallelContext * pcxt,
                                void *coordinate)
{
    ParallelCoordinator        *coord = (ParallelCoordinator *) coordinate;
    ParquetS3FdwExecutionState   *festate;

    /*
    coord->i.s.next_rowgroup = 0;
    coord->i.s.next_reader = 0;
    SpinLockInit(&coord->lock);
    */
    festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    festate->set_coordinator(coord);
    festate->init_coord();
}

extern "C" void
parquetReInitializeDSMForeignScan(ForeignScanState *node,
                                  ParallelContext * /* pcxt */,
                                  void * /* coordinate */)
{
    ParquetS3FdwExecutionState   *festate;

    festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    festate->init_coord();
}

extern "C" void
parquetInitializeWorkerForeignScan(ForeignScanState *node,
                                   shm_toc * /* toc */,
                                   void *coordinate)
{
    ParallelCoordinator        *coord   = (ParallelCoordinator *) coordinate;
    ParquetS3FdwExecutionState   *festate;

    coord = new(coordinate) ParallelCoordinator;
    festate = (ParquetS3FdwExecutionState *) node->fdw_state;
    festate->set_coordinator(coord);
}

extern "C" void
parquetShutdownForeignScan(ForeignScanState * /* node */)
{
}

extern "C" List *
parquetImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    struct dirent  *f;
    DIR            *d;
    List           *cmds = NIL;
    char           *dirname = pstrdup(stmt->remote_schema);
    char           *back;

    if (IS_S3_PATH(stmt->remote_schema))
        return parquetImportForeignSchemaS3(stmt, serverOid);

    d = AllocateDir(stmt->remote_schema);
    if (!d)
    {
        int e = errno;

        elog(ERROR, "parquet_s3_fdw: failed to open directory '%s': %s",
             stmt->remote_schema,
             strerror(e));
    }

    /* remove redundant slash */
    back = dirname + strlen(dirname);
    while (*--back == '/')
    {
        *back = '\0';
    }

    while ((f = readdir(d)) != NULL)
    {

        /* TODO: use lstat if d_type == DT_UNKNOWN */
        if (f->d_type == DT_REG)
        {
            ListCell   *lc;
            bool        skip = false;
            List       *fields;
            char       *filename = pstrdup(f->d_name);
            char       *path;
            char       *query;

            path = psprintf("%s/%s", dirname, filename);

            /* check that file extension is "parquet" */
            char *ext = strrchr(filename, '.');

            if (ext && strcmp(ext + 1, "parquet") != 0)
                continue;

            /*
             * Set terminal symbol to be able to run strcmp on filename
             * without file extension
             */
            *ext = '\0';

            foreach (lc, stmt->table_list)
            {
                RangeVar *rv = (RangeVar *) lfirst(lc);

                switch (stmt->list_type)
                {
                    case FDW_IMPORT_SCHEMA_LIMIT_TO:
                        if (strcmp(filename, rv->relname) != 0)
                        {
                            skip = true;
                            break;
                        }
                        break;
                    case FDW_IMPORT_SCHEMA_EXCEPT:
                        if (strcmp(filename, rv->relname) == 0)
                        {
                            skip = true;
                            break;
                        }
                        break;
                    default:
                        ;
                }
            }
            if (skip)
                continue;

            fields = extract_parquet_fields(path, NULL, NULL);

            query = create_foreign_table_query(filename, stmt->local_schema,
                                               stmt->server_name, &path, 1,
                                               fields, stmt->options);
            cmds = lappend(cmds, query);
        }

    }
    FreeDir(d);

    return cmds;
}

extern "C" Datum
parquet_fdw_validator_impl(PG_FUNCTION_ARGS)
{
    List       *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid         catalog = PG_GETARG_OID(1);
    ListCell   *lc;
    bool        filename_provided = false;
    bool        func_provided = false;

    /* Only check table options */
    if (catalog != ForeignTableRelationId)
        PG_RETURN_VOID();

    foreach(lc, options_list)
    {
        DefElem    *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "filename") == 0)
        {
            char   *filename = pstrdup(defGetString(def));
            List   *filenames;
            ListCell *lc;

            if (filename_provided)
                elog(ERROR, "parquet_s3_fdw: either filename or dirname can be specified");

            filenames = parse_filenames_list(filename);

            foreach(lc, filenames)
            {
                struct stat stat_buf;
                char       *fn = strVal((Node *) lfirst(lc));

               if (IS_S3_PATH(fn))
                   continue;

                if (stat(fn, &stat_buf) != 0)
                {
                    int e = errno;

                    ereport(ERROR,
                            (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                             errmsg("parquet_s3_fdw: %s ('%s')", strerror(e), fn)));
                }
            }
            pfree(filenames);
            pfree(filename);
            filename_provided = true;
        }
        else if (strcmp(def->defname, "files_func") == 0)
        {
            Oid     jsonboid = JSONBOID;
            List   *funcname = stringToQualifiedNameList(defGetString(def)); 
            Oid     funcoid;
            Oid     rettype;

            /*
             * Lookup the function with a single JSONB argument and fail
             * if there isn't one.
             */
            funcoid = LookupFuncName(funcname, 1, &jsonboid, false);
            if ((rettype = get_func_rettype(funcoid)) != TEXTARRAYOID)
            {
                elog(ERROR, "parquet_s3_fdw: return type of '%s' is %s; expected text[]",
                     defGetString(def), format_type_be(rettype));
            }
            func_provided = true;
        }
        else if (strcmp(def->defname, "files_func_arg") == 0)
        {
            /* 
             * Try to convert the string value into JSONB to validate it is
             * properly formatted.
             */
            DirectFunctionCall1(jsonb_in, CStringGetDatum(defGetString(def)));
        }
        else if (strcmp(def->defname, "sorted") == 0)
            ;  /* do nothing */
        else if (strcmp(def->defname, "use_mmap") == 0)
        {
            /* Check that bool value is valid */
            (void) defGetBoolean(def);
        }
        else if (strcmp(def->defname, "use_threads") == 0)
        {
            /* Check that bool value is valid */
            (void) defGetBoolean(def);
        }
        else if (strcmp(def->defname, "dirname") == 0)
        {
            char *dirname = defGetString(def);

            if (filename_provided)
                elog(ERROR, "parquet_s3_fdw: either filename or dirname can be specified");

            if (!IS_S3_PATH(dirname))
            {
                struct stat stat_buf;

                if (stat(dirname, &stat_buf) != 0)
                {
                    int e = errno;

                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("parquet_s3_fdw: %s", strerror(e))));
                }
                if (!S_ISDIR(stat_buf.st_mode))
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("parquet_s3_fdw: %s is not a directory", dirname)));

            }
            filename_provided = true;
        }
        else if (parquet_s3_is_valid_server_option(def))
        {
            /* Do nothing. */
        }
        else if (strcmp(def->defname, "max_open_files") == 0)
        {
            /* check that int value is valid */
#if PG_VERSION_NUM >= 150000
            pg_strtoint32(defGetString(def));
#else
            pg_atoi(defGetString(def), sizeof(int32), '\0');
#endif
        }
        else if (strcmp(def->defname, "files_in_order") == 0)
        {
            /* Check that bool value is valid */
			(void) defGetBoolean(def);
        }
        else if (strcmp(def->defname, "schemaless") == 0)
        {
            /* Check that bool value is valid */
			(void) defGetBoolean(def);
        }
        else if (strcmp(def->defname, "insert_file_selector") == 0)
        {
            /* We does not have foreign table type here, so do nothing */
        }
        else if (strcmp(def->defname, "key_columns") == 0)
        {
            /* We does not have foreign table type here, so do nothing */
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("parquet_s3_fdw: invalid option \"%s\"",
                            def->defname)));
        }
    }

    if (!filename_provided && !func_provided)
        elog(ERROR, "parquet_s3_fdw: filename or function is required");

    PG_RETURN_VOID();
}

static List *
jsonb_to_options_list(Jsonb *options)
{
    List           *res = NIL;
	JsonbIterator  *it;
    JsonbValue      v;
    JsonbIteratorToken  type = WJB_DONE;

    if (!options)
        return NIL;

    if (!JsonContainerIsObject(&options->root))
        elog(ERROR, "parquet_s3_fdw: options must be represented by a jsonb object");

    it = JsonbIteratorInit(&options->root);
    while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    {
        switch (type)
        {
            case WJB_BEGIN_OBJECT:
            case WJB_END_OBJECT:
                break;
            case WJB_KEY:
                {
                    DefElem    *elem;
                    char       *key;
                    char       *val;

                    if (v.type != jbvString)
                        elog(ERROR, "parquet_s3_fdw: expected a string key");
                    key = pnstrdup(v.val.string.val, v.val.string.len);

                    /* read value directly after key */
                    type = JsonbIteratorNext(&it, &v, false);
                    if (type != WJB_VALUE || v.type != jbvString)
                        elog(ERROR, "parquet_s3_fdw: expected a string value");
                    val = pnstrdup(v.val.string.val, v.val.string.len);

                    elem = makeDefElem(key, (Node *) makeString(val), 0);
                    res = lappend(res, elem);

                    break;
                }
            default:
                elog(ERROR, "parquet_s3_fdw: wrong options format");
        }
    }

    return res;
}

static List *
array_to_fields_list(ArrayType *attnames, ArrayType *atttypes)
{
    List   *res = NIL;
    Datum  *names;
    Datum  *types;
    bool   *nulls;
    int     nnames;
    int     ntypes;

    if (!attnames || !atttypes)
        elog(ERROR, "parquet_s3_fdw: attnames and atttypes arrays must not be NULL");

    if (ARR_HASNULL(attnames))
        elog(ERROR, "parquet_s3_fdw: attnames array must not contain NULLs");

    if (ARR_HASNULL(atttypes))
        elog(ERROR, "parquet_s3_fdw: atttypes array must not contain NULLs");

    deconstruct_array(attnames, TEXTOID, -1, false, 'i', &names, &nulls, &nnames);
    deconstruct_array(atttypes, REGTYPEOID, 4, true, 'i', &types, &nulls, &ntypes);

    if (nnames != ntypes)
        elog(ERROR, "parquet_s3_fdw: attnames and attypes arrays must have same length");

    for (int i = 0; i < nnames; ++i)
    {
        FieldInfo  *field = (FieldInfo *) palloc(sizeof(FieldInfo));
        char       *attname;
        attname = text_to_cstring(DatumGetTextP(names[i]));

        if (strlen(attname) >= NAMEDATALEN)
            elog(ERROR, "parquet_s3_fdw: attribute name cannot be longer than %i", NAMEDATALEN - 1);

        strcpy(field->name, attname);
        field->oid = types[i];

        res = lappend(res, field);
    }

    return res;
}

static void
validate_import_args(const char *tablename, const char *servername, Oid funcoid)
{
    if (!tablename)
        elog(ERROR, "parquet_s3_fdw: foreign table name is mandatory");

    if (!servername)
        elog(ERROR, "parquet_s3_fdw: foreign server name is mandatory");

    if (!OidIsValid(funcoid))
        elog(ERROR, "parquet_s3_fdw: function must be specified");
}

static void
import_parquet_internal(const char *tablename, const char *schemaname,
                        const char *servername, List *fields, Oid funcid,
                        Jsonb *arg, Jsonb *options) noexcept
{
    Datum       res;
    FmgrInfo    finfo;
    ArrayType  *arr;
    Oid         ret_type;
    List       *optlist;
    char       *query;

    validate_import_args(tablename, servername, funcid);

    if ((ret_type = get_func_rettype(funcid)) != TEXTARRAYOID)
    {
        elog(ERROR,
             "parquet_s3_fdw: return type of '%s' function is %s; expected text[]",
             get_func_name(funcid), format_type_be(ret_type));
    }

    optlist = jsonb_to_options_list(options);

    /* Call the user provided function */
    fmgr_info(funcid, &finfo);
    res = FunctionCall1(&finfo, (Datum) arg);

    /*
     * In case function returns NULL the ERROR is thrown. So it's safe to
     * assume function returned something. Just for the sake of readability
     * I leave this condition
     */
    if (res != (Datum) 0)
    {
        Datum  *values;
        bool   *nulls;
        int     num;
        int     ret;

        arr = DatumGetArrayTypeP(res);
        deconstruct_array(arr, TEXTOID, -1, false, 'i', &values, &nulls, &num);

        if (num == 0)
        {
            elog(WARNING,
                 "parquet_s3_fdw: '%s' function returned an empty array; foreign table wasn't created",
                 get_func_name(funcid));
            return;
        }

        /* Convert values to cstring array */
        char **paths = (char **) palloc(num * sizeof(char *));
        for (int i = 0; i < num; ++i)
        {
            if (nulls[i])
                elog(ERROR, "parquet_s3_fdw: user function returned an array containing NULL value(s)");
            paths[i] = text_to_cstring(DatumGetTextP(values[i]));
        }

        /*
         * If attributes list is provided then use it. Otherwise get the list
         * from the first file provided by the user function. We trust the user
         * to provide a list of files with the same structure.
         */
        fields = parquetExtractParquetFields(fields, paths, servername);

        query = create_foreign_table_query(tablename, schemaname, servername,
                                           paths, num, fields, optlist);

        /* Execute query */
        if (SPI_connect() < 0)
            elog(ERROR, "parquet_s3_fdw: SPI_connect failed");

        if ((ret = SPI_exec(query, 0)) != SPI_OK_UTILITY)
            elog(ERROR, "parquet_s3_fdw: failed to create table '%s': %s",
                 tablename, SPI_result_code_string(ret));

        SPI_finish();
    }
}

extern "C"
{

PG_FUNCTION_INFO_V1(import_parquet_s3);

Datum
import_parquet_s3(PG_FUNCTION_ARGS)
{
    char       *tablename;
    char       *schemaname;
    char       *servername;
    Oid         funcid;
    Jsonb      *arg;
    Jsonb      *options;

    tablename = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
    schemaname = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));
    servername = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(2));
    funcid = PG_ARGISNULL(3) ? InvalidOid : PG_GETARG_OID(3);
    arg = PG_ARGISNULL(4) ? NULL : PG_GETARG_JSONB_P(4);
    options = PG_ARGISNULL(5) ? NULL : PG_GETARG_JSONB_P(5);

    import_parquet_internal(tablename, schemaname, servername, NULL, funcid, arg, options);

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(import_parquet_s3_with_attrs);

Datum
import_parquet_s3_with_attrs(PG_FUNCTION_ARGS)
{
    char       *tablename;
    char       *schemaname;
    char       *servername;
    ArrayType  *attnames;
    ArrayType  *atttypes;
    Oid         funcid;
    Jsonb      *arg;
    Jsonb      *options;
    List       *fields;
    bool        schemaless = false;
    ListCell   *lc;

    tablename = PG_ARGISNULL(0) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(0));
    schemaname = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));
    servername = PG_ARGISNULL(2) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(2));
    attnames = PG_ARGISNULL(3) ? NULL : PG_GETARG_ARRAYTYPE_P(3);
    atttypes = PG_ARGISNULL(4) ? NULL : PG_GETARG_ARRAYTYPE_P(4);
    funcid = PG_ARGISNULL(5) ? InvalidOid : PG_GETARG_OID(5);
    arg = PG_ARGISNULL(6) ? NULL : PG_GETARG_JSONB_P(6);
    options = PG_ARGISNULL(7) ? NULL : PG_GETARG_JSONB_P(7);


    foreach(lc, jsonb_to_options_list(options))
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "schemaless") == 0)
			schemaless = defGetBoolean(def);
    }

    if (schemaless)
    {
        if (attnames != NULL || atttypes != NULL)
            ereport(WARNING,
                    errmsg("parquet_s3_fdw: Attnames and atttypes are expected to be NULL. They are meaningless for schemaless table."),
                    errhint("Schemaless table imported always contain \"v\" column with \"jsonb\" type."));
        fields = NIL;
    }
    else
    {
        fields = array_to_fields_list(attnames, atttypes);
    }

    import_parquet_internal(tablename, schemaname, servername, fields,
                            funcid, arg, options);

    PG_RETURN_VOID();
}

/*
 * parquetPlanForeignModify
 *        Plan an insert/update/delete operation on a foreign table
 */
extern "C" List *
parquetPlanForeignModify(PlannerInfo *root,
                         ModifyTable *plan,
                         Index resultRelation,
                         int subplan_index)
{
    CmdType         operation = plan->operation;
    RangeTblEntry  *rte = planner_rt_fetch(resultRelation, root);
    List           *targetAttrs = NULL;
    Relation        rel;
    List           *keyAttrs = NULL;
    Oid             foreignTableId;
    TupleDesc       tupdesc;

    /*
     * Core code already has some lock on each rel being planned, so we can
     * use NoLock here.
     */
    rel = table_open(rte->relid, NoLock);
    foreignTableId = RelationGetRelid(rel);
    tupdesc = RelationGetDescr(rel);

    /*
     * In an INSERT, we transmit all columns that are defined in the foreign
     * table.  In an UPDATE, if there are BEFORE ROW UPDATE triggers on the
     * foreign table, we transmit all columns like INSERT; else we transmit
     * only columns that were explicitly targets of the UPDATE, so as to avoid
     * unnecessary data transmission.  (We can't do that for INSERT since we
     * would miss sending default values for columns not listed in the source
     * statement, and for UPDATE if there are BEFORE ROW UPDATE triggers since
     * those triggers might change values for non-target columns, in which
     * case we would miss sending changed values for those columns.)
     */
    if (operation == CMD_INSERT ||
        (operation == CMD_UPDATE &&
         rel->trigdesc &&
         rel->trigdesc->trig_update_before_row))
    {
        int     attnum;

        for (attnum = 1; attnum <= tupdesc->natts; attnum++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

            if (!attr->attisdropped)
                targetAttrs = lappend_int(targetAttrs, attnum);
        }
    }
    else if (operation == CMD_UPDATE)
    {
        AttrNumber  col;
        Bitmapset  *allUpdatedCols = bms_union(rte->updatedCols, rte->extraUpdatedCols);

        col = -1;
        while ((col = bms_next_member(allUpdatedCols, col)) >= 0)
        {
            /* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
            AttrNumber  attno = col + FirstLowInvalidHeapAttributeNumber;

            if (attno <= InvalidAttrNumber) /* shouldn't happen */
                elog(ERROR, "parquet_s3_fdw: system-column update is not supported");
            targetAttrs = lappend_int(targetAttrs, attno);
        }
    }

    /*
     * Raise error message if there is WITH CHECK OPTION
     */
    if (plan->withCheckOptionLists)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                 errmsg("parquet_s3_fdw: unsupported feature WITH CHECK OPTION")));
    }

    /*
     * Raise error if there is RETURNING
     */
    if (plan->returningLists)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                 errmsg("parquet_s3_fdw: unsupported feature RETURNING")));
    }

    /*
     * Raise error if there is ON CONFLICT
     */
    if (plan->onConflictAction != ONCONFLICT_NONE)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                 errmsg("parquet_s3_fdw: unsupported feature ON CONFLICT")));
    }

    /*
     * Add all primary key attribute names to keyAttrs
     */
    for (int i = 0; i < tupdesc->natts; ++i)
    {
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        AttrNumber    attrno = att->attnum;
        List       *options;
        ListCell   *option;

        /* look for the "key" option on this column */
        options = GetForeignColumnOptions(foreignTableId, attrno);
        foreach(option, options)
        {
            DefElem    *def = (DefElem *) lfirst(option);

            if (IS_KEY_COLUMN(def))
            {
                keyAttrs = lappend(keyAttrs, makeString(att->attname.data));
            }
        }
    }

    table_close(rel, NoLock);
    return list_make2(targetAttrs, keyAttrs);
}

// single table test
static ParquetS3FdwModifyState *fmstate = NULL;

extern "C" void
ParquetDmlInit(Relation rel) {
    //Oid    foreignTableId = InvalidOid;
    TupleDesc               tupleDesc;
    MemoryContextCallback  *callback;
    std::string             error;
    arrow::Status           status;
    std::set<std::string>   sorted_cols;
    std::set<std::string>   key_attrs;
    std::set<int>           target_attrs;
	MemoryContext           temp_cxt;

    bool use_threads = true;
    bool schemaless = true;
	//ListCell lc;

    //Oid tableId = RelationGetRelid(rel);
    tupleDesc = RelationGetDescr(rel);

    std::vector<std::string> filenames;

	// TODO
    for (int i = 0; i < tupleDesc->natts; ++i) {
        target_attrs.insert(i);
	}

    char dirname[100];
    sprintf(dirname, "%d/%d/", rel->rd_node.dbNode, rel->rd_node.relNode);

	temp_cxt = AllocSetContextCreate(NULL,
								  "parquet_s3_fdw temporary data",
								  ALLOCSET_DEFAULT_SIZES);

    auto s3client = ParquetGetConnectionByRelation(rel);
    try {
        fmstate = create_parquet_modify_state(temp_cxt, dirname, s3client, tupleDesc,
                                                 target_attrs, key_attrs, NULL, use_threads,
                                                 use_threads, schemaless, sorted_cols);

        fmstate->set_rel_name(RelationGetRelationName(rel));
        for (size_t i = 0; i < filenames.size(); ++i) {
            fmstate->add_file(filenames[i].data());
        }

        //if (plstate->selector_function_name)
        //    fmstate->set_user_defined_func(plstate->selector_function_name);
    } catch(std::exception &e) {
        error = e.what();
    }
    if (!error.empty()) {
        elog(ERROR, "parquet_s3_fdw: %s", error.c_str());
    }

    /*
     * Enable automatic execution state destruction by using memory context
     * callback
     */
    callback = (MemoryContextCallback *) palloc(sizeof(MemoryContextCallback));
    callback->func = destroy_parquet_modify_state;
    callback->arg = (void *) fmstate;
    //MemoryContextRegisterResetCallback(estate->es_query_cxt, callback);
}

extern "C" void
ParquetDmlFinish(Relation rel) {
    //ParquetS3FdwModifyState *fmstate = NULL;

    //Oid                     foreignTableId = InvalidOid;
    //foreignTableId = RelationGetRelid(rel);
}

/*
 * parquetBeginForeignModify
 *      Begin an insert/update/delete operation on a foreign table
 */
extern "C" void
parquetBeginForeignModify(ModifyTableState *mtstate,
                          ResultRelInfo *resultRelInfo,
                          List *fdw_private,
                          int subplan_index,
                          int eflags)
{
    ParquetS3FdwModifyState *fmstate = NULL;
    EState	               *estate = mtstate->ps.state;
    Relation                rel = resultRelInfo->ri_RelationDesc;
    Oid                     foreignTableId = InvalidOid;
    std::string             error;
    arrow::Status           status;
    Plan                   *subplan;
    ParquetFdwPlanState    *plstate;
    int                     i = 0;
    ListCell               *lc;
    MemoryContext           temp_cxt;
    TupleDesc               tupleDesc;
    std::set<std::string>   sorted_cols;
    std::set<std::string>   key_attrs;
    std::set<int>           target_attrs;
    MemoryContextCallback  *callback;

#if (PG_VERSION_NUM >= 140000)
    subplan = outerPlanState(mtstate)->plan;
#else
    subplan = mtstate->mt_plans[subplan_index]->plan;
#endif

    temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                     "parquet_s3_fdw temporary data",
                                     ALLOCSET_DEFAULT_SIZES);
    foreignTableId = RelationGetRelid(rel);

    /* get table option */
    plstate = (ParquetFdwPlanState *) palloc0(sizeof(ParquetFdwPlanState));
    get_table_options(foreignTableId, plstate);

    /* 
     * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
     * stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Get S3 connection */
    if (IS_S3_PATH(plstate->dirname) || parquetIsS3Filenames(plstate->filenames))
        plstate->s3client = parquetGetConnectionByTableid(foreignTableId);
    else
        plstate->s3client = NULL;

    if (!plstate->filenames)
    {
        if (IS_S3_PATH(plstate->dirname))
            plstate->filenames = parquetGetS3ObjectList(plstate->s3client, plstate->dirname);
        else
            plstate->filenames = parquetGetDirFileList(plstate->filenames, plstate->dirname);
    }

    tupleDesc = RelationGetDescr(rel);

    foreach(lc, (List *) list_nth(fdw_private, 0))
        target_attrs.insert(lfirst_int(lc));

    AttrNumber *junk_idx = (AttrNumber *)palloc0(RelationGetDescr(rel)->natts * sizeof(AttrNumber));

    if (plstate->schemaless == true)
    {
        foreach(lc, plstate->key_columns)
        {
            char *key_column_name = (char *) lfirst(lc);

            key_attrs.insert(std::string(key_column_name));
        }
    }
    else
    {
        List        *key_attrs_name_list = (List *) list_nth(fdw_private, 1);

        foreach(lc, key_attrs_name_list)
        {
            char *key_column_name = strVal((Node *) lfirst(lc));

            key_attrs.insert(std::string(key_column_name));
        }
    }

    if (mtstate->operation != CMD_INSERT)
    {
        if (key_attrs.size() == 0)
            elog(ERROR, "parquet_s3_fdw: no primary key column specified for foreign table");
    }

    /* loop through table columns */
    for (i = 0; i < RelationGetDescr(rel)->natts; ++i)
    {
        /*
         * for primary key columns, get the resjunk attribute number and store it
         */
        junk_idx[i] = ExecFindJunkAttributeInTlist(subplan->targetlist,
                                         get_attname(foreignTableId, i + 1, false));
    }

    /*
     *  Get sorted column list.
     */
    foreach (lc, plstate->attrs_sorted)
    {
        char   *attname = (char *)lfirst(lc);

        sorted_cols.insert(std::string(attname));
    }

    try
    {
        fmstate = create_parquet_modify_state(temp_cxt, plstate->dirname, plstate->s3client, tupleDesc,
                                                 target_attrs, key_attrs, junk_idx, plstate->use_threads,
                                                 plstate->use_threads, plstate->schemaless, sorted_cols);

        fmstate->set_rel_name(RelationGetRelationName(rel));
        foreach(lc, plstate->filenames)
        {
            char *filename = strVal((Node *) lfirst(lc));

            fmstate->add_file(filename);
        }

        if (plstate->selector_function_name)
            fmstate->set_user_defined_func(plstate->selector_function_name);
    }
    catch(std::exception &e)
    {
        error = e.what();
    }
    if (!error.empty())
        elog(ERROR, "parquet_s3_fdw: %s", error.c_str());

    /*
     * Enable automatic execution state destruction by using memory context
     * callback
     */
    callback = (MemoryContextCallback *) palloc(sizeof(MemoryContextCallback));
    callback->func = destroy_parquet_modify_state;
    callback->arg = (void *) fmstate;
    MemoryContextRegisterResetCallback(estate->es_query_cxt, callback);

    resultRelInfo->ri_FdwState = fmstate;
}

extern "C" void
ParquetTupleInsert(Relation rel, TupleTableSlot *slot,
				   CommandId cid, int options,
				   struct BulkInsertStateData *bistate)
{
    fmstate->exec_insert(slot);
    //return slot;
}

/*
 * parquetExecForeignInsert
 *      Insert one row into a foreign table
 */
extern "C" TupleTableSlot *
parquetExecForeignInsert(EState *estate,
                         ResultRelInfo *resultRelInfo,
                         TupleTableSlot *slot,
                         TupleTableSlot *planSlot)
{
    ParquetS3FdwModifyState *fmstate = (ParquetS3FdwModifyState *) resultRelInfo->ri_FdwState;

    fmstate->exec_insert(slot);

    return slot;
}


/*
 * parquetExecForeignUpdate
 *      Update one row in a foreign table
 */
extern "C" TM_Result
ParquetTupleUpdate(Relation rel,
                    ItemPointer otid,
                    TupleTableSlot *slot,
                    CommandId cid,
                    Snapshot snapshot,
                    Snapshot crosscheck,
                    bool wait,
                    TM_FailureData *tmfd,
                    LockTupleMode *lockmode,
                    bool *update_indexes)
{
    fmstate->exec_update(slot, NULL);

    return TM_Ok;
}
/*
 * parquetExecForeignUpdate
 *      Update one row in a foreign table
 */
extern "C" TupleTableSlot *
parquetExecForeignUpdate(EState *estate,
                          ResultRelInfo *resultRelInfo,
                          TupleTableSlot *slot,
                          TupleTableSlot *planSlot)
{
    ParquetS3FdwModifyState *fmstate = (ParquetS3FdwModifyState *) resultRelInfo->ri_FdwState;

    fmstate->exec_update(slot, planSlot);

    return slot;
}

/*
 * postgresExecForeignDelete
 *      Delete one row from a foreign table
 */
extern "C" TupleTableSlot *
parquetExecForeignDelete(EState *estate,
                         ResultRelInfo *resultRelInfo,
                         TupleTableSlot *slot,
                         TupleTableSlot *planSlot)
{
    ParquetS3FdwModifyState *fmstate = (ParquetS3FdwModifyState *) resultRelInfo->ri_FdwState;

    fmstate->exec_delete(slot, planSlot);

    return slot;
}

/*
 * postgresEndForeignModify
 *      Finish an insert/update/delete operation on a foreign table
 */
extern "C" void
parquetEndForeignModify(EState *estate,
                        ResultRelInfo *resultRelInfo)
{
    ParquetS3FdwModifyState   *fmstate = (ParquetS3FdwModifyState *) resultRelInfo->ri_FdwState;

    if (fmstate != NULL)
        fmstate->upload();
}

/*
 * parquet_slot_to_record_datum: Convert TupleTableSlot to record type
 */
Datum
parquet_slot_to_record_datum(TupleTableSlot *slot)
{
    HeapTuple tuple = heap_form_tuple(slot->tts_tupleDescriptor, slot->tts_values, slot->tts_isnull);

    return HeapTupleHeaderGetDatum(tuple->t_data);
}

inline std::string trim(std::string str)
{
    str.erase(str.find_last_not_of(' ') + 1);   /* suffixing spaces */
    str.erase(0, str.find_first_not_of(' '));   /* prefixing spaces */
    return str;
}

}
