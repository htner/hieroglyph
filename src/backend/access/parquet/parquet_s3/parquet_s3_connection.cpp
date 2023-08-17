/*-------------------------------------------------------------------------
 *
 * parquet_s3_fdw_connection.c
 *		  Connection management functions for parquet_s3_fdw
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/parquet_s3_fdw_connection.c
 *
 *-------------------------------------------------------------------------
 */
#include "backend/access/parquet/parquet_s3/parquet_s3.hpp"

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


static Aws::SDKOptions *aws_sdk_options;

extern void parquet_s3_init() {
	aws_sdk_options = new Aws::SDKOptions();
	Aws::InitAPI(*aws_sdk_options);
}

extern void parquet_s3_shutdown() {
	Aws::ShutdownAPI(*aws_sdk_options);
	aws_sdk_options = NULL;
}

/*
 * Release connection reference count created by calling GetConnection.
 */
void parquetReleaseConnection(Aws::S3::S3Client *conn) {
	/*
   * Currently, we don't actually track connection references because all
   * cleanup is managed on a transaction or subtransaction basis instead. So
   * there's nothing to do here.
   */
}


/*
 * Create S3 handle.
 */
Aws::S3::S3Client *s3_client_open(const char *user, const char *password,
								  bool use_minio, const char *endpoint,
								  const char *awsRegion) {
	const Aws::String access_key_id = user;
	const Aws::String secret_access_key = password;
	Aws::Auth::AWSCredentials cred =
		Aws::Auth::AWSCredentials(access_key_id, secret_access_key);
	Aws::S3::S3Client *s3_client;

	Aws::Client::ClientConfiguration clientConfig;

	if (use_minio) {
		const Aws::String defaultEndpoint = "127.0.0.1:9000";
		clientConfig.scheme = Aws::Http::Scheme::HTTP;
		clientConfig.endpointOverride =
			endpoint ? (Aws::String)endpoint : defaultEndpoint;
		s3_client = new Aws::S3::S3Client(
			cred, clientConfig,
			Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
	} else {
		const Aws::String defaultRegion = "ap-northeast-1";
		clientConfig.scheme = Aws::Http::Scheme::HTTPS;
		clientConfig.region = awsRegion ? (Aws::String)awsRegion : defaultRegion;
		s3_client = new Aws::S3::S3Client(cred, clientConfig);
	}
	return s3_client;
}

/*
 * Close S3 handle.
 */
extern void s3_client_close(Aws::S3::S3Client *s3_client) { delete s3_client; }


/*
 * Get file names in S3 directory. Retuned file names are path from s3path.
 */
std::list<std::string> parquetGetS3ObjectList(Aws::S3::S3Client *s3_cli, const char *s3path) {
	std::list<std::string> keys;
	//List *objectlist = NIL;
	Aws::S3::S3Client s3_client = *s3_cli;
	Aws::S3::Model::ListObjectsRequest request;

	if (s3path == NULL) return keys;

	/* Calculate bucket name and directory name from S3 path. */
	const char *bucket = s3path + 5;       /* Remove "s3://" */
	const char *dir = strchr(bucket, '/'); /* Search the 1st '/' after "s3://". */
	const Aws::String &bucketName = bucket;
	size_t len;
	if (dir) {
		len = dir - bucket;
		dir++; /* Remove '/' */
	} else {
		len = bucketName.length();
	}
	request.WithBucket(bucketName.substr(0, len));

	auto outcome = s3_client.ListObjects(request);

	if (!outcome.IsSuccess()) {

	}
	/*
	elog(ERROR, "parquet_s3_fdw: failed to get object list on %s. %s",
		 bucketName.substr(0, len).c_str(),
		 outcome.GetError().GetMessage().c_str());
	*/

	Aws::Vector<Aws::S3::Model::Object> objects =
		outcome.GetResult().GetContents();
	for (Aws::S3::Model::Object &object : objects) {
		Aws::String key = object.GetKey();
		if (!dir) {
			// objectlist =
		//		lappend(objectlist, makeString(pstrdup((char *)key.c_str())));
			keys.push_back(key.c_str());
			// elog(DEBUG1, "parquet_s3_fdw: accessing %s%s", s3path, key.c_str());
		} else if (strncmp(key.c_str(), dir, strlen(dir)) == 0) {
			std::string file = (char *)key.substr(strlen(dir)).c_str();
			/* Don't register if the object is directory. */
			if (file.at(key.length() - 1) != '/' && strcmp(file.data(), "/") != 0) {
				// objectlist = lappend(objectlist, makeString(file));
				keys.push_back(file);
				/*
		elog(DEBUG1, "parquet_s3_fdw: accessing %s%s", s3path,
			 key.substr(strlen(dir)).c_str());
				*/
			}
		} else {

			/*
	  elog(DEBUG1, "parquet_s3_fdw: skipping s3://%s/%s",
		   bucketName.substr(0, len).c_str(), key.c_str());
			*/
		}
	}

	return keys;
}

/*
 * Split s3 path into bucket name and file path.
 * If foreign table option 'dirname' is specified, dirname starts by
 * "s3://". And filename is already set by get_filenames_in_dir().
 * On the other hand, if foreign table option 'filename' is specified,
 * dirname is NULL (Or empty string when ANALYZE was executed)
 * and filename is set as fullpath started by "s3://".
 */
void parquetSplitS3Path(const char *dirname, const char *filename,
						std::string *bucket, std::string *filepath) {

	*bucket = dirname;
	*filepath = filename;
}
