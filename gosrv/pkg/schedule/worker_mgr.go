package schedule

import (
	"log"
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/proto/sdb"
	"google.golang.org/protobuf/proto"
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
  // workers map[int32]sdb.WorkerStatus  cache in the future
}

func NewWorkerMgr() *WorkerMgr {
  return &WorkerMgr{segid: 1, supportMultiTask: false}
}

// not need to get all
func (mgr *WorkerMgr) GetWorkerList(clusterId uint32, count int) ([]*sdb.WorkerStatus, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	data, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		var key keys.WorkerStatusKey = keys.WorkerStatusKey{Cluster: clusterId}

		sKeyStart, err := fdbkv.MarshalRangePerfix(&key)
		if err != nil {
			log.Printf("marshal ranage perfix %v", err)
			return nil, err
		}

    rangekey, err := fdb.PrefixRange(sKeyStart)
		rr := tr.GetRange(rangekey, fdb.RangeOptions{})
		ri := rr.Iterator()

		// Advance will return true until the iterator is exhausted
		workers := make([]*sdb.WorkerStatus, 0)
		for ri.Advance() {
			s := &sdb.WorkerStatus{}
			data, e := ri.Get()
			log.Println(data)
			if e != nil {
				log.Printf("Unable to read next value: %v\n", e)
				return nil, nil
			}
			proto.Unmarshal(data.Value, s)
			if err != nil {
				log.Printf("Unmarshal error %v", err)
				return nil, err
			}
      if s.WorkerState == sdb.WorkerState_WSReady {
        s.WorkerState = sdb.WorkerState_WSBusy
			  workers = append(workers, s)
        if len(workers) == count {
          break
        }
      }
		}
    if len(workers) != count {
      return nil, errors.New("count not match")
    }
    kvOp := fdbkv.NewKvOperator(tr)
    for _, w := range workers {
		  var key keys.WorkerStatusKey = keys.WorkerStatusKey{Cluster: clusterId, WorkerId: w.WorkerId}
      kvOp.WritePB(&key, w)
    }
		return workers, nil
	})

	if err != nil {
		return nil, err
	}
	return data.([]*sdb.WorkerStatus), nil
  //return workers, nil 
}

func (mgr *WorkerMgr) GetServerList(clusterId uint32, num int32, sliceIndex int32) ([]*sdb.WorkerInfo, []*sdb.WorkerSliceInfo, error) {
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

  workers, err := mgr.GetWorkerList(clusterId, int(num))
  if err != nil {
    return nil, nil, err
  }

  for _, worker := range workers {
    workinfo := &sdb.WorkerInfo{
      Addr:  worker.Addr,
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

func (mgr *WorkerMgr) Ping(req *sdb.WorkerPingRequest) error {
  db, err := fdb.OpenDefault()
  if err != nil {
    log.Println("ping open err: ", err)
    return err
  }
  /*
var mgr LockMgr
var fdblock Lock
fdblock.Database = L.T.Database
fdblock.Relation = rel
fdblock.LockType = InsertLock
fdblock.Sid = L.T.Sid
*/
  _, err = db.Transact(
    func(tr fdb.Transaction) (interface{}, error) {

      kvOp := fdbkv.NewKvOperator(tr)
      status := sdb.WorkerStatus{Cluster: req.Cluster, WorkerId: req.WorkerId, WorkerState: req.State, CurrectIndex: req.CurrectIndex, Addr:req.Addr}
      key := keys.WorkerStatusKey{Cluster: req.Cluster, WorkerId: req.WorkerId}

      var oldStatus sdb.WorkerStatus

      err = kvOp.ReadPB(&key, &oldStatus)

      if err != nil {
		    if err != fdbkv.ErrEmptyData {
			    return nil, err
		    }
      } else {
        if oldStatus.CurrectIndex > status.CurrectIndex {
          return nil, errors.New("")
        }

        if oldStatus.WorkerState > status.WorkerState && oldStatus.CurrectIndex == status.CurrectIndex {
          return nil, errors.New("")
        }
      }
      err = kvOp.WritePB(&key, &status)
      return nil, err
    })
  return err
}
