package schedule

import (
	"fmt"
	"log"

	"github.com/htner/sdb/gosrv/proto/sdb"
)

const (
	basePort = 9100

	GANGTYPE_UNALLOCATED      = 0
	GANGTYPE_ENTRYDB_READER   = 1
	GANGTYPE_SINGLETON_READER = 2
	GANGTYPE_PRIMARY_READER   = 3
	GANGTYPE_PRIMARY_WRITER   = 4
)

type WorkerMgr struct {
	segid            int32
	supportMultiTask bool
}

func NewWorkerMgr() *WorkerMgr {
	return &WorkerMgr{segid: 1, supportMultiTask: false}
}

func (mgr *WorkerMgr) GetServerList(num int32, sliceIndex int32) ([]*sdb.WorkerInfo, []*sdb.WorkerSliceInfo, error) {
	var workerSlices []*sdb.WorkerSliceInfo
	var workinfos []*sdb.WorkerInfo
	var segid *int32
	// if can multi-threads
	if !mgr.supportMultiTask {
		segid = &(mgr.segid)
	} else {
		segid = new(int32)
		*segid = int32(1)
	}
	var i int32
	for i = 0; i < num; i++ {
		workinfo := &sdb.WorkerInfo{
			Addr:  fmt.Sprintf("127.0.0.1:%d", basePort+(*segid)),
			Id:    int64(*segid + 100000),
			Segid: *segid,
		}

		workerslice := &sdb.WorkerSliceInfo{
			WorkerInfo: workinfo,
			Sliceid:    int32(sliceIndex),
		}
		workerSlices = append(workerSlices, workerslice)
		workinfos = append(workinfos, workinfo)
		*segid++
	}

	log.Println("dddtest GetServerSliceList workinfos ", workinfos)
	log.Println("dddtest GetServerSliceList workerSlice ", workerSlices)
	return workinfos, workerSlices, nil
}
