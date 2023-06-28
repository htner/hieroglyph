package kvpair

const (
	SessionTag         uint16 = 10
	TranscationTag     uint16 = 11
	SessionTickTag     uint16 = 16
  TransactionChangeRelKeyTag uint16 = 17
	SeqTag             uint16 = 18
	LockTag            uint16 = 19
	LakeLogKeyTag         uint16 = 20
	LakeLogItemKeyTag     uint16 = 21
	LakeFileTag        uint16 = 22
	CLOGTag            uint16 = 23
	FileListVersionTag uint16 = 24

  QueryStatusTag uint16 = 30
  QueryRequestTag uint16 = 31
  QueryOptimizerResultTag uint16 = 32
  QueryWorkerRequestTag uint16 = 33
  QueryWorkerDetailTag uint16 = 34
  QueryResultTag uint16 = 35

	MAXTIDTag          uint16 = 40
	MAXFILEIDTag          uint16 = 41
	MaxDeleteVersionTag uint16 = 42
)
