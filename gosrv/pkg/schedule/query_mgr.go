package schedule

import (
	"errors"
	"log"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

type QueryMgr struct {
	Database uint64
}

func NewQueryMgr(dbid uint64) *QueryMgr {
	return &QueryMgr{Database: dbid}
}
func (mgr *QueryMgr) NewQuery() *keys.QueryKey {
	var key keys.QueryKey
	key.Dbid = uint64(mgr.Database)
	//key.Commandid = cid
	//key.QueryTag = tag
	return &key
}

/*
func (mgr *QueryMgr) NewQueryKey(cid uint64, tag uint16) *QueryKey {
	var key keys.QueryKey
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

		key := keys.NewQueryKey(uint64(mgr.Database), queryId, keys.QueryRequestTag)
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

		key := keys.NewQueryKey(uint64(mgr.Database), queryId, keys.QueryOptimizerResultTag)
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

		key := keys.NewQueryKey(uint64(mgr.Database), req.TaskIdentify.QueryId, keys.QueryWorkerRequestTag)
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

		key := &keys.WorkerTaskKey{
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

func (mgr *QueryMgr) InitQueryResult(queryId uint64, state uint32, root_workers uint32, msg string) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

		key := keys.NewQueryKey(uint64(mgr.Database), queryId, keys.QueryResultTag)
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
    value.Message = new(sdb.ErrorResponse)
		value.Message.Message = msg
		value.Message.Code = "00000" 

		value.RootWorkers = root_workers

		return nil, kvOp.WritePB(key, value)
	})
	return e
}

func (mgr *QueryMgr) QueryError(queryId uint64, state uint32, code string, message string) error {
  var e sdb.ErrorResponse
  e.Code = code
  e.Severity = "ERROR"
  e.Message = message
  return mgr.WriteErrorQueryResult(queryId, state, &e)
}

func (mgr *QueryMgr) WriteErrorQueryResult(queryId uint64, state uint32, message *sdb.ErrorResponse) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		kvOp := fdbkv.NewKvOperator(tr)

		key := keys.NewQueryKey(uint64(mgr.Database), queryId, keys.QueryResultTag)
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
		value.RootWorkers = 1 
    value.Message = message 

    result := new(sdb.WorkerResultData)
    //result.SqlErrCode = sqlerrcode
    result.Message = message 

		value.Result = append(value.Result, result)

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

		key := keys.NewQueryKey(mgr.Database, queryId, keys.QueryResultTag)
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

    log.Println(key, value)
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
		key := keys.NewQueryKey(uint64(mgr.Database), queryId, keys.QueryResultTag)
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
