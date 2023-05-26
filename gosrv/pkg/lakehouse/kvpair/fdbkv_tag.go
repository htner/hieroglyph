package kvpair

// for mvcc
const (
	// 内部使用
	SessionTag     uint16 = 10
	TranscationTag uint16 = 11
	CatalogKVTag   uint16 = 12
	UserKVTag      uint16 = 14
	WALLogTag      uint16 = 15
	SessionTickTag uint16 = 16
	MAXTIDTag      uint16 = 17
	SeqTag         uint16 = 18
	LockTag        uint16 = 19

	// MVCC使用
	SchemaKVTag   uint16 = 257
	LakeFileTag   uint16 = 259
)
