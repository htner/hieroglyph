package schedule

import (
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

type QueryMgr struct {
	Database types.DatabaseId
}

func NewQueryMgr(dbid types.DatabaseId) *QueryMgr {
	return &QueryMgr{Database: dbid}
}
func (mgr *QueryMgr) NewQuery() *kvpair.QueryKey {
	var key kvpair.QueryKey
	key.Dbid = uint64(mgr.Database)
	//key.Commandid = cid
	//key.QueryTag = tag
	return &key
}

/*
func (mgr *QueryMgr) NewQueryKey(cid uint64, tag uint16) *QueryKey {
	var key kvpair.QueryKey
	key.Dbid = uint64(mgr.Database)
	key.Commandid = cid
	key.QueryTag = tag
	return &key
}
*/

func (mgr *QueryMgr) WriterQueryDetail(req *sdb.ExecQueryRequest, queryId uint64) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

		key := kvpair.NewQueryKey(uint64(mgr.Database), queryId, kvpair.QueryRequestTag)
		value := new(sdb.QueryRequestInfo)
		value.QueryRequest = req
		value.CreateTimestamp = time.Now().UnixMicro()
		err = kvOp.WritePB(key, value)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	return e
}

func (mgr *QueryMgr) WriterOptimizerResult(req *sdb.OptimizeReply, queryId uint64) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

		key := kvpair.NewQueryKey(uint64(mgr.Database), queryId, kvpair.QueryOptimizerResultTag)
		value := new(sdb.QueryOptimizerResult)
		value.OptimizerResult = req
		err = kvOp.WritePB(key, value)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	return e
}

func (mgr *QueryMgr) WriterWorkerInfo(req *sdb.PrepareTaskRequest) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

		key := kvpair.NewQueryKey(uint64(mgr.Database), req.TaskIdentify.QueryId, kvpair.QueryWorkerRequestTag)
		value := new(sdb.QueryWorkerInfo)
		value.PrepareTaskInfo = req
		err = kvOp.WritePB(key, value)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return e
}

func (mgr *QueryMgr) WriterWorkerResult(req *sdb.PushWorkerResultRequest) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

    key := &kvpair.WorkerTaskKey{
      Dbid:uint64(mgr.Database), 
      QueryId:req.TaskId.QueryId, 
      SliceId: req.TaskId.SliceId,
      SegId: req.TaskId.SegId,
     }
		value := new(sdb.TaskResult)
		value.TaskIdentify = req.TaskId
		value.Result = req.Result
		err = kvOp.WritePB(key, value)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	return e
}

func (mgr *QueryMgr) WriteQueryResult(queryId uint64, state uint32, msg, detail string, result *sdb.WorkerResultData) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

		key := kvpair.NewQueryKey(uint64(mgr.Database), queryId, kvpair.QueryResultTag)
		value := new(sdb.QueryResult)
		value.Result = result
    value.Message = detail
    value.State = state
		err = kvOp.WritePB(key, value)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	return e
}

func (mgr *QueryMgr) ReadQueryResult(queryId uint64) (*sdb.QueryResult, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	value, e := db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {

		kvOp := fdbkv.NewKvReader(tr)

		key := kvpair.NewQueryKey(uint64(mgr.Database), queryId, kvpair.QueryResultTag)
		value := new(sdb.QueryResult)
		err = kvOp.ReadPB(key, value)
		if err != nil {
			return nil, err
		}

		return value, nil
	})
	if e != nil {
		return nil, e
	}
	return value.(*sdb.QueryResult), e
}
