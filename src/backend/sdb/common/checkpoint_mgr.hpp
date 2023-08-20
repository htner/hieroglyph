#progma once

namespace sdb {

class CheckpointMgr {
public:
  CheckpointMgr(const std::string& sql) = default;
	virtual ~CheckpointMgr() = default; 

	void Sync(std::vector<std::CatalogItemRequest> items_request);

  const std::TableFiles& GetTableFiles(uint64_t oid);
private:
  std::unordered_map<uint64_t, sdb::TableFiles> cur_info_;
};

}
