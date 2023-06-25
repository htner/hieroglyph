package schedule

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/proto"
  "encoding/binary"
  "bytes"
)

type QueryMgr struct {
	Database types.DatabaseId
}

func NewQueryMgr(dbid types.DatabaseId)*QueryMgr {
	return &QueryMgr{Database: dbid}
}

type QueryKey struct {
  commandid uint64 
  tag uint16
}

func NewQueryKey(cid uint64, tag uint16) *QueryKey {
  var key QueryKey
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

func (mgr *QueryMgr) WriterQueryDetail (req *proto.ExecQueryRequest) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

	  kvOp := fdbkv.NewKvOperator(tr)

    key := NewQueryKey(req.Commandid, kvpair.QueryRequestTag)
    value := new(proto.CommandDetails)
    value.Request = req
		err = kvOp.WritePB(key, value)
		if err != nil {
			return nil, err
		}

    err = mgr.UpdateQueryStatusInTran(tr, req.Commandid, proto.CommandStatus_CS_INIT)
		if err != nil {
			return nil, err
		}
		//tick := &kv.SessionTick{Id: t.Sid, LastTick: time.Now().UnixMicro()}
		//return nil, kvOp.Write(tick, tick)
    return nil, nil
	})
	return e
}

func (mgr *QueryMgr) UpdateQueryStatus (commandid uint64, status proto.CommandStatus) error {
  db, err := fdb.OpenDefault()
  if err != nil {
    return err
  }
  _, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    err = mgr.UpdateQueryStatusInTran(tr, commandid, proto.CommandStatus_CS_INIT)
    if err != nil {
      return nil, err
    }
    return nil, nil
    })
  return e
}

func (mgr *QueryMgr) UpdateQueryStatusInTran (tr fdb.Transaction,  commandid uint64, status proto.CommandStatus) error {

  kvOp := fdbkv.NewKvOperator(tr)

  keyStatus := NewQueryKey(commandid, kvpair.QueryStatusTag)
  valueStatus := new(proto.CommandStatusDetail)
  valueStatus.Status = status 
  return kvOp.WritePB(keyStatus, valueStatus)
}

func (mgr *QueryMgr) WriterOptimizerResult(commandid uint64, req *proto.OptimizeReply) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

	  kvOp := fdbkv.NewKvOperator(tr)

    key := NewQueryKey(commandid, kvpair.QueryOptimizerResultTag)
    value := new(proto.CommandOptimizerResult)
    value.OptimizerResult = req
		err = kvOp.WritePB(key, value)
		if err != nil {
			return nil, err
		}

    err = mgr.UpdateQueryStatusInTran(tr, commandid, proto.CommandStatus_CS_OP)
		if err != nil {
			return nil, err
		}
		//tick := &kv.SessionTick{Id: t.Sid, LastTick: time.Now().UnixMicro()}
		//return nil, kvOp.Write(tick, tick)
    return nil, nil
	})
	return e
}

func (mgr *QueryMgr) WriterExecDetail(req *proto.PrepareTaskRequest) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

	  kvOp := fdbkv.NewKvOperator(tr)

    key := NewQueryKey(req.TaskIdentify.QueryId, kvpair.QueryOptimizerResultTag)
    value := new(proto.CommandExecDetail)
    value.PrepareTaskInfo = req
		err = kvOp.WritePB(key, value)
		if err != nil {
			return nil, err
		}

    err = mgr.UpdateQueryStatusInTran(tr, req.TaskIdentify.QueryId, proto.CommandStatus_CS_WAIT_EXEC)
		if err != nil {
			return nil, err
		}
		//tick := &kv.SessionTick{Id: t.Sid, LastTick: time.Now().UnixMicro()}
		//return nil, kvOp.Write(tick, tick)
    return nil, nil
	})
	return e
}

func (mgr *QueryMgr) WriterExecResult(req *proto.WorkerResultReportRequest) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

	  kvOp := fdbkv.NewKvOperator(tr)

    key := NewQueryKey(req.TaskId.QueryId, kvpair.QueryOptimizerResultTag)
    value := new(proto.CommandResult)
    value.Result = req
		err = kvOp.WritePB(key, value)
		if err != nil {
			return nil, err
		}

    err = mgr.UpdateQueryStatusInTran(tr, req.TaskId.QueryId, proto.CommandStatus_CS_DONE)
		if err != nil {
			return nil, err
		}
		//tick := &kv.SessionTick{Id: t.Sid, LastTick: time.Now().UnixMicro()}
		//return nil, kvOp.Write(tick, tick)
    return nil, nil
	})
	return e
}
