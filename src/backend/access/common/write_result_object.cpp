
extern "C" {
#include "access/write_result_object.h"
}

#include <arpa/inet.h>
#include <filesystem>
#include "backend/access/parquet/parquet_s3/parquet_s3.hpp"
#include <string>
#include <memory>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fstream>

extern std::string kResultBucket;
extern std::string kResultS3User;
extern std::string kResultS3Password;
extern std::string kResultS3Region;
extern std::string kResultS3Endpoint;
extern bool kResultIsMinio;

namespace fs = std::filesystem;

uint64_t htonll(uint64_t x)
{
#if __BIG_ENDIAN__
    return x;
#else
    return ((uint64_t)htonl((x) & 0xFFFFFFFFLL) << 32) | htonl((x) >> 32);
#endif
}

uint64_t ntohll(uint64_t x)
{
#if __BIG_ENDIAN__
    return x;
#else
    return ((uint64_t)ntohl((x) & 0xFFFFFFFFLL) << 32) | ntohl((x) >> 32);
#endif
}

class ObjectStream {
public:
	ObjectStream(const char *dirname, const char *filename);
	~ObjectStream() = default;

	bool Upload();

	int WriteResultToFile(char msgtype, const char *buf, int size);
	
	void Flush();

	void EndStream();

private:
	std::string buff_;
	std::string dirname_;
	std::string filename_;
	std::string local_file_;
	std::unique_ptr<Aws::S3::S3Client> s3_client_;
	std::unique_ptr<Aws::SDKOptions> aws_sdk_options_;
	std::unique_ptr<std::fstream> file_stream_;
};

ObjectStream::ObjectStream(const char *dirname, const char *filename)
  : dirname_(dirname), filename_(filename) {
  aws_sdk_options_ = std::make_unique<Aws::SDKOptions>();
  Aws::InitAPI(*aws_sdk_options_);

  s3_client_.reset(s3_client_open(
	 kResultS3User.data(), kResultS3Password.data(), kResultIsMinio, kResultS3Endpoint.data(), kResultS3Region.data()));
      //"minioadmin", "minioadmin", true, "127.0.0.1:9000", "ap-northeast-1"));
  local_file_ = dirname_ + "/" + filename_;

  fs::path filepath (local_file_);
  if (fs::exists(filepath)) {
	fs::remove(filepath);
  }

  file_stream_ = std::make_unique<std::fstream> (local_file_,
		  			std::fstream::out | std::fstream::binary | std::fstream::app);
}

bool ObjectStream::Upload() {
  char *filepath;
  Aws::S3::Model::PutObjectRequest request;
  std::shared_ptr<Aws::IOStream> input_data;
  Aws::S3::Model::PutObjectOutcome outcome;

  filepath = local_file_.data();
  request.SetBucket(kResultBucket.data());

  /*
   * We are using the name of the file as the key for the object in the bucket.
   */
  request.SetKey(filepath);

  /* load local file to update */
  input_data =
      Aws::MakeShared<Aws::FStream>("PutObjectInputStream", local_file_,
                                    std::ios_base::in | std::ios_base::binary);

  request.SetBody(input_data);
  outcome = s3_client_->PutObject(request);

  if (outcome.IsSuccess()) {
    elog(WARNING, "parquet_s3_fdw: added object %s (%s, %s) to bucket %s", filepath,
         dirname_.c_str(), local_file_.c_str(), kResultBucket.data());
    return true;
  } else {
    elog(ERROR, "parquet_s3_fdw: PutObject: %s",
         outcome.GetError().GetMessage().c_str());
    return false;
  }
}

int ObjectStream::WriteResultToFile(char msgtype, const char *buf, int size) {

	std::filesystem::path p(local_file_);
	std::filesystem::path dir = p.parent_path();
	if (!std::filesystem::exists(dir)) {
		std::filesystem::create_directory(dir);
	}
	if (!file_stream_->is_open()) {
		file_stream_->open(local_file_,
				std::fstream::out | std::fstream::binary | std::fstream::app);
	}

	if (!file_stream_->is_open()) {
		elog(PANIC, "open file failed reason %d, %d",
						file_stream_->rdstate(),
						file_stream_->exceptions());
		return -1;
	}

	// we will write tuple or desc to file, the format is:
	// size(int64) + msgtype + tuple/desc
	uint64_t total_size = htonll(size + 1);
	file_stream_->write(reinterpret_cast<char *>(&total_size), sizeof(uint64_t));
	file_stream_->write(&msgtype, 1);
	file_stream_->write(buf, size);
	return 0;
}

void ObjectStream::Flush() {
	Assert(file_stream_ != nullptr);
	file_stream_->flush();
}

void ObjectStream::EndStream() {
	if (file_stream_ != nullptr && file_stream_->is_open()) {
		file_stream_->flush();
		file_stream_->close();
	}

	this->Upload();
}

static ObjectStream *object_stream = nullptr;

void
CreateObjectStream(const char* dirname, const char *filename)
{
	Assert(object_stream == nulltpr);
	object_stream = new ObjectStream(dirname, filename);
}

int
WriteResultToObject(char msgtype, const char *buf, int size)
{
	Assert(object_stream != nullptr);
	return object_stream->WriteResultToFile(msgtype, buf, size);
}

void
WriteResultEnd()
{
	if (object_stream != nullptr) {
		object_stream->EndStream();
		delete object_stream;
		object_stream = nullptr;
	}
}

void
WriteResultFlush()
{
	Assert(object_stream != nullptr);
	object_stream->Flush();
}
