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

func (mgr *QueryMgr) WriterQueryDetail(req *sdb.ExecQueryRequest) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

		key := mgr.NewQueryKey(req.Commandid, kvpair.QueryRequestTag)
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

func (mgr *QueryMgr) WriterOptimizerResult(commandid uint64, req *sdb.OptimizeReply) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

		key := mgr.NewQueryKey(commandid, kvpair.QueryOptimizerResultTag)
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

		key := mgr.NewQueryKey(req.TaskIdentify.QueryId, kvpair.QueryWorkerRequestTag)
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

		key := mgr.NewQueryKey(req.TaskId.QueryId, kvpair.QueryWorkerDetailTag)
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

		key := mgr.NewQueryKey(queryId, kvpair.QueryResultTag)
		value := new(sdb.QueryResult)
		value.Result = result
		err = kvOp.WritePB(key, value)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	return e
}

func (mgr *QueryMgr) ReadQueryResult(queryId uint64) (*sdb.WorkerResultData, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	value, e := db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {

		kvOp := fdbkv.NewKvReader(tr)

		key := mgr.NewQueryKey(queryId, kvpair.QueryResultTag)
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
	return value.(*sdb.WorkerResultData), e
}
