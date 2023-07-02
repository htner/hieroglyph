package schedule

import (
	"fmt"
	"log"

	"github.com/htner/sdb/gosrv/proto/sdb"
)

type WorkerMgr struct {
}

func (mgr *WorkerMgr) GetServerList() ([]*sdb.WorkerInfo, error) {
	return nil, nil
}

/*
 * expand slices to slice list
 */
func (mgr *WorkerMgr) GetServerSliceList(slices []*sdb.PBPlanSlice) ([]*sdb.WorkerInfo, []*sdb.WorkerSliceInfo, error) {
	seg := int32(1)
	totalSeg := int32(0)
	var workerSlices []*sdb.WorkerSliceInfo
	var workinfos []*sdb.WorkerInfo

	for i, slice := range slices {
		if int32(i) != slice.SliceIndex {
			log.Fatalf("slice index not match %d.%s", i, slice.String())
		}

		totalSeg += slice.NumSegments;
		for ; seg <= totalSeg; seg++ {
			workinfo := &sdb.WorkerInfo{
				Addr:  fmt.Sprintf("127.0.0.1:%d", 40000+seg),
				Id:    int64(seg),
				Segid: seg,
			}

			workerslice := &sdb.WorkerSliceInfo{
				WorkerInfo: workinfo,
				Sliceid:    slice.SliceIndex,
			}
			workerSlices = append(workerSlices, workerslice)
			workinfos = append(workinfos, workinfo)
		}
	}

	return workinfos, workerSlices, nil
}
