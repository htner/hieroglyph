/*-------------------------------------------------------------------------
 *
 * parquet_s3_fdw.hpp
 *		  Header file of accessing S3 module for parquet_s3_fdw
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/parquet_s3_fdw.hpp
 *
 *-------------------------------------------------------------------------
 */

#ifndef __PARQUET_S3_HPP__
#define __PARQUET_S3_HPP__ 1 

#pragma once

// #include "backend/sdb/common/pg_export.hpp"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <arrow/api.h>
#include <parquet/arrow/reader.h>

class S3RandomAccessFile : public arrow::io::RandomAccessFile {
private:
	Aws::String bucket_;
	Aws::String object_;
	Aws::S3::S3Client *s3_client_;
	int64_t offset;
	bool isclosed;

public:
	S3RandomAccessFile(Aws::S3::S3Client *s3_client,
					   const Aws::String &bucket, const Aws::String &object);

	arrow::Status Close();
	arrow::Result<int64_t>Tell() const;
	bool closed() const;
	arrow::Status Seek(int64_t position);
	arrow::Result<int64_t> Read(int64_t nbytes, void* out);
	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes);
	arrow::Result<int64_t> GetSize();
};
#ifdef FDB_NOUSE

typedef enum FileLocation_t {
    LOC_NOT_DEFINED,
    LOC_LOCAL,
    LOC_S3
} FileLocation;

/*
 * We would like to cache FileReader. When creating new hash entry,
 * the memory of entry is allocated by PostgreSQL core. But FileReader is
 * a unique_ptr. In order to initialize it in parquet_s3_fdw, we define 
 * FileReaderCache class and the cache entry has the pointer of this class.
 */
class FileReaderCache {
	public:
		std::unique_ptr<parquet::arrow::FileReader> reader;
};

typedef struct ReaderCacheKey {
	char dname[256];
	char fname[256];
} ReaderCacheKey;

typedef struct ReaderCacheEntry {
	ReaderCacheKey key;			/* hash key (must be first) */
	FileReaderCache *file_reader;
	arrow::MemoryPool *pool;
} ReaderCacheEntry;

extern List *extract_parquet_fields(const char *path, const char *dirname, Aws::S3::S3Client *s3_client) noexcept;
extern char *create_foreign_table_query(const char *tablename, const char *schemaname, const char *servername,
                                         char **paths, int npaths, List *fields, List *options);

extern List* parquetGetS3ObjectList(Aws::S3::S3Client *s3_cli, const char *s3path);
extern List* parquetGetDirFileList(List *filelist, const char *path);
extern FileLocation parquetFilenamesValidator(const char *filename, FileLocation loc);
extern bool parquetIsS3Filenames(List *filenames);
extern void parquet_disconnect_s3_server();

#endif
extern std::unique_ptr<parquet::arrow::FileReader> 
parquetGetFileReader(Aws::S3::S3Client *s3client, const char *dname, const char *fname);

extern Aws::S3::S3Client *s3_client_open(const char *user, const char *password,
                                         bool use_minio, const char *endpoint,
                                         const char *awsRegion);

extern void s3_client_close(Aws::S3::S3Client *s3_client);

extern arrow::Status CopyToLocalFile(Aws::S3::S3Client *s3_client,
					   const Aws::String &bucket, const Aws::String &object, const std::string& filename);


extern void parquetSplitS3Path(const char *dirname, const char *filename, std::string *bucket, std::string *filepath);

extern bool parquet_upload_file_to_s3(const char *dirname, Aws::S3::S3Client *s3_client, 
                                      const char *filename, const char *local_file);

#define IS_S3_PATH(str) (str != NULL && strncmp(str, "s3://", 5) == 0)

#endif
