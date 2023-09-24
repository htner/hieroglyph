package schedule

import (
	"fmt"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
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

func (mgr *WorkerMgr) WorkerProcPing(workerProc *sdb.WorkerProcInfo) (err error) {
	// 上锁
	db, err := fdb.OpenDefault()
	if err != nil {
		log.Println("PrepareFiles open err: ", err)
		return err
	}

	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvOp := NewKvOperator(tr)

		var key keys.FreeWorkerListItemTag
		key.ClusterId = workerProc.ClusterId
		key.WorkerId = workerProc.WorkerId

		if workerProc.Status == WS_Free {
			err = kvOp.WritePB(&key, &file)
			if err != nil {
				return nil, err
			}
		}
	})
	if e != nil {
		log.Println("PrepareFiles lock err: ", e)
	}
	return e
}

func (mgr *WorkerMgr) GetServer(num int32) ([]*sdb.WorkerProcInfo) {
	data, err := db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		var key keys.FileKey = keys.FileKey{Database: L.T.Database, Relation: rel, Fileid: 0}

		sKeyStart, err := fdbkv.MarshalRangePerfix(&key)
		if err != nil {
			log.Printf("marshal ranage perfix %v", err)
			return nil, err
		}
		key.Fileid = math.MaxUint64
		sKeyEnd, err := fdbkv.MarshalRangePerfix(&key)
		if err != nil {
			log.Printf("marshal ranage perfix %v", err)
			return nil, err
		}

		keyStart := fdb.Key(sKeyStart)
		keyEnd := fdb.Key(sKeyEnd)
		rr := tr.GetRange(fdb.KeyRange{Begin: keyStart, End: keyEnd},
			fdb.RangeOptions{Limit: 10000})
		ri := rr.Iterator()
	}
}


func (mgr *WorkerMgr) GetServerList(num int32) ([]*sdb.WorkerInfo, error) {
	workinfos := make([]*sdb.WorkerInfo, 0)
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
		workinfos = append(workinfos, workinfo)
		*segid++
	}

	log.Println("dddtest GetServerSliceList workinfos ", workinfos)
	return workinfos, nil
}
