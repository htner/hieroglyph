package schedule

import (
	"fmt"
	"log"

	"github.com/htner/sdb/gosrv/proto/sdb"
)

const (
	basePort = 9100
  
  GANGTYPE_UNALLOCATED = 0
  GANGTYPE_ENTRYDB_READER = 1
  GANGTYPE_SINGLETON_READER = 2
  GANGTYPE_PRIMARY_READER = 3
  GANGTYPE_PRIMARY_WRITER = 4
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
	if len(slices) == 0 {
		workinfo := &sdb.WorkerInfo{
			Addr:  fmt.Sprintf("127.0.0.1:%d", basePort+seg),
			Id:    int64(seg),
			Segid: seg,
		}

		workerslice := &sdb.WorkerSliceInfo{
			WorkerInfo: workinfo,
			Sliceid:    0,
		}

		workerSlices = append(workerSlices, workerslice)
		workinfos = append(workinfos, workinfo)
		return workinfos, workerSlices, nil
	}

	for i, slice := range slices {
		if int32(i) != slice.SliceIndex {
			log.Fatalf("slice index not match %d.%s", i, slice.String())
		}

    writeGangSegNum := -1
    // for insert, we should create gang randomly
    if (slice.GangType == GANGTYPE_UNALLOCATED ||
        slice.GangType == GANGTYPE_ENTRYDB_READER ||
        slice.GangType == GANGTYPE_SINGLETON_READER ||
        slice.GangType == GANGTYPE_PRIMARY_WRITER) {
      writeGangSegNum = 1
      writeGangSegNum += int(totalSeg)
    }

		totalSeg += slice.NumSegments
		for ; seg <= totalSeg; seg++ {
			workinfo := &sdb.WorkerInfo{
				Addr:  fmt.Sprintf("127.0.0.1:%d", basePort+seg),
				Id:    int64(seg),
				Segid: seg,
			}

      if (writeGangSegNum == -1 || seg == int32(writeGangSegNum)) {
			  workerslice := &sdb.WorkerSliceInfo{
				  WorkerInfo: workinfo,
				  Sliceid:    slice.SliceIndex,
			  }
			  workerSlices = append(workerSlices, workerslice)
      }
			workinfos = append(workinfos, workinfo)
		}
	}

	log.Println("dddtest GetServerSliceList 2 ", workerSlices)
	return workinfos, workerSlices, nil
}
