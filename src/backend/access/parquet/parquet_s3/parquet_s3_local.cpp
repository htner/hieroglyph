
#include "backend/access/parquet/parquet_s3/parquet_s3.hpp"
#include <fstream>
#include <filesystem>

#include <parquet/arrow/reader.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <arrow/io/file.h>
#include <dirent.h>
#include <sys/stat.h>

#include <butil/logging.h>

/*
 * Get a S3 handle which can be used to get objects on AWS S3
 * with the user's authorization.  A new connection is established
 * if we don't already have a suitable one.
 */
std::unique_ptr<parquet::arrow::FileReader>
parquetGetFileReader(Aws::S3::S3Client *s3client, const char *dname,
					 const char *fname) {
	std::unique_ptr<parquet::arrow::FileReader> reader;
	/*
	std::shared_ptr<arrow::io::RandomAccessFile> input(
		new S3RandomAccessFile(s3client, dname, fname));
	arrow::Status status =
		parquet::arrow::OpenFile(input, entry->pool, &reader);
	*/
	std::string filename = "base/tmp/";
	std::filesystem::create_directory(filename);

	filename += fname;
	if (!std::filesystem::exists(filename)) {
		auto result = CopyToLocalFile(s3client, dname, fname, filename);
		if (!result.ok()) {
			//elog(DEBUG3, "copy to local file error: %s", result.ToString().data());
			return reader;
		}
	}

	auto result = arrow::io::ReadableFile::Open(filename, arrow::default_memory_pool());
	if (!result.status().ok()) {
		//elog(DEBUG3, "open file error: %s", result.status().ToString().data());
		return reader;
	}
	std::shared_ptr<arrow::io::ReadableFile> infile = result.ValueOrDie();
	arrow::Status status = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);

	if (!status.ok()) {
		// throw Error("parquetGetFileReader: failed to open Parquet file %s", status.message().c_str());
		return nullptr;
	}
	return reader;
}

bool parquet_upload_file_to_s3(const char *dirname,
							   Aws::S3::S3Client *s3_client,
							   const char *filename, const char *local_file) {
	std::string bucket;
	std::string filepath;
	Aws::S3::Model::PutObjectRequest request;
	std::shared_ptr<Aws::IOStream> input_data;
	Aws::S3::Model::PutObjectOutcome outcome;

	parquetSplitS3Path(dirname, filename, &bucket, &filepath);
	request.SetBucket(bucket);

	/*
   * We are using the name of the file as the key for the object in the bucket.
   */
	request.SetKey(filepath);

	/* load local file to update */
	input_data =
		Aws::MakeShared<Aws::FStream>("PutObjectInputStream", local_file,
								std::ios_base::in | std::ios_base::binary);

	request.SetBody(input_data);
	outcome = s3_client->PutObject(request);

	if (outcome.IsSuccess()) {
		// elog(WARNING, "parquet_s3_fdw: added object %s (%s, %s) to bucket %s", filepath,
		//     dirname, local_file, bucket);
		return true;
	} else {
		LOG(ERROR) << "parquet_s3_fdw: PutObject:" <<
			outcome.GetError().GetMessage().c_str() << " bucket:" << bucket;
		return false;
	}
}
