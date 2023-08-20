package main

type QueryUuidMaker struct {
	ProxyServerId uint64
	serial        uint64
}

func NewQueryUUIDMaker(serverid uint64) *QueryUuidMaker {
	return &QueryUuidMaker{ProxyServerId: serverid, serial: 0}
}

func (M *QueryUuidMaker) MakeQueryId() uint64 {
	// FIX from
	M.serial++
	//return fmt.Sprintf("%d-%d-%d", M.ProxyServerId, time.Now().UnixMicro(), M.serial)
	return M.ProxyServerId<<32 + M.serial
}
