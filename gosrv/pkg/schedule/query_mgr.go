package schedule

import (
	"time"
	"bytes"
	"encoding/binary"

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

type QueryKey struct {
	dbid uint64
	commandid uint64
	tag       uint16
}

func (mgr *QueryMgr) NewQueryKey(cid uint64, tag uint16) *QueryKey {
	var key QueryKey
  key.dbid = uint64(mgr.Database)
	key.commandid = cid
	key.tag = tag
	return &key
}

func (k *QueryKey) Tag() uint16 {
	return k.tag
}

func (k *QueryKey) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, k.commandid)
}

func (k *QueryKey) DecFdbKey(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &k.commandid)
}

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

func (mgr *QueryMgr) WriterQueryResult(queryId uint64, state uint32, msg, detail string, result *sdb.WorkerResultData) error {
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
