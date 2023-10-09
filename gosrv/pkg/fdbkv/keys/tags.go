package keys

const (
	SessionTag                 uint16 = 10
	TranscationTag             uint16 = 11
	SessionTickTag             uint16 = 16
	TransactionChangeRelKeyTag uint16 = 17
	SeqTag                     uint16 = 18
	LockTag                    uint16 = 19
	LakeLogKeyTag              uint16 = 20
	LakeLogItemKeyTag          uint16 = 21
	LakeFileTag                uint16 = 22
	CLOGTag                    uint16 = 23
	FileListVersionTag         uint16 = 24
	PotentialLockTag           uint16 = 25
	WorkerStatusTag            uint16 = 26

	QueryStatusTag          uint16 = 30
	QueryRequestTag         uint16 = 31
	QueryOptimizerResultTag uint16 = 32
	QueryWorkerRequestTag   uint16 = 33
	QueryWorkerDetailTag    uint16 = 34
	QueryResultTag          uint16 = 35
	WorkerResultTag         uint16 = 36


	MAXTIDTag           uint16 = 40
	MAXFILEIDTag        uint16 = 41
	MaxDeleteVersionTag uint16 = 42

	SdbAccountKeyTag       uint16 = 45
	AccountLoginNameKeyTag uint16 = 46
	OrganizationKeyTag     uint16 = 47
	OrganizationNameKeyTag uint16 = 48
	UserKeyTag             uint16 = 49
	LoginNameKeyTag        uint16 = 50

	DatabaseKeyTag     uint16 = 51
	DatabaseNameKeyTag uint16 = 52

	// first class object id gen
	FCAccountMaxIDTag      uint16 = 100
	FCOrganizatoinMaxIDTag uint16 = 101
	FCUserMaxIDTag         uint16 = 102
	FCDatabaseMaxIDTag     uint16 = 103
	SessionMaxIDTag        uint16 = 104
	DatabaseObjectIDTag    uint16 = 105
	FileSharedObjectIDTag    uint16 = 106
)
