package schedule

import (
	"log"

	"github.com/htner/sdb/gosrv/proto/sdb"
)

type WorkerMgr struct {
}

func (mgr *WorkerMgr) GetServerList() ([]*sdb.WorkerInfo, error) {
	return nil, nil
}

func (mgr *WorkerMgr) GetServerSliceList(slices []*sdb.PBPlanSlice) ([]*sdb.WorkerInfo, []*sdb.WorkerSliceInfo, error) {
	for i, slice := range slices {
		if int32(i) != slice.SliceIndex {
			log.Fatalf("slice index not match %d.%s", i, slice.String())
		}
	}
	return nil, nil, nil
}
