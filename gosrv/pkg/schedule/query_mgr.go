package schedule

import (
	"errors"
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
			Dbid:    uint64(mgr.Database),
			QueryId: req.TaskId.QueryId,
			SliceId: req.TaskId.SliceId,
			SegId:   req.TaskId.SegId,
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

func (mgr *QueryMgr) InitQueryResult(queryId uint64, state uint32, root_workers uint32) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

		key := kvpair.NewQueryKey(uint64(mgr.Database), queryId, kvpair.QueryResultTag)
		value := new(sdb.QueryResult)

		err = kvOp.ReadPB(key, value)
		if err != nil {
			if err != fdbkv.ErrEmptyData {
				return nil, err
			}
			if value.State >= state {
				return nil, errors.New("state rollback?")
			}
		}

		value.State = state
		value.Message = ""
		value.Detail = ""
		value.RootWorkers = root_workers

		return nil, kvOp.WritePB(key, value)
	})
	return e
}

func (mgr *QueryMgr) WriteQueryResult(queryId uint64, result *sdb.WorkerResultData) (bool, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return false, err
	}
	isFinish, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

		key := kvpair.NewQueryKey(uint64(mgr.Database), queryId, kvpair.QueryResultTag)
		value := new(sdb.QueryResult)

		err = kvOp.ReadPB(key, value)
		if err != nil {
			return false, err
		}

		value.State = uint32(sdb.QueryStates_QueryPartitionSuccess)
		value.Result = append(value.Result, result)

		isFinish := false
		if len(value.Result) >= int(value.RootWorkers) {
			value.State = uint32(sdb.QueryStates_QuerySuccess)
			isFinish = true
		}

		err = kvOp.WritePB(key, value)
		return isFinish, err
	})
	return isFinish.(bool), err
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
