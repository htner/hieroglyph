package lakehouse

import (
	"errors"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/lakehouse/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

const (
	XS_NULL   types.XState = types.XState(0)
	XS_INIT   types.XState = types.XState(1)
	XS_START  types.XState = types.XState(2)
	XS_COMMIT types.XState = types.XState(3)
	XS_ABORT  types.XState = types.XState(4)
)

const InvaildTranscaton types.TransactionId = 0

type Transaction struct {
	Database types.DatabaseId
	Xid      types.TransactionId // write xid
	Sid      types.SessionId
}

func NewTranscation(dbid types.DatabaseId, sid types.SessionId) *Transaction {
	return &Transaction{Xid: InvaildTranscaton,
		Database: dbid, Sid: sid}
}

func NewTranscationWithXid(dbid types.DatabaseId, xid types.TransactionId, sid types.SessionId) *Transaction {
	return &Transaction{Xid: xid, Database: dbid, Sid: sid}
}

// 使用 fdb 的原则， 启动一个事务
func (t *Transaction) Start(autoCommit bool) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvOp := NewKvOperator(tr)
		sess := kv.NewSession(t.Sid)
		err := kvOp.Read(sess, sess)
		if err != nil {
			return nil, err
		}
		if sess.State != kv.SessionTransactionIdle {
			return nil, errors.New("session in transaction")
		}
		sess.AutoCommit = autoCommit
		sess.State = kv.SessionTransactionStart
		err = kvOp.Write(sess, sess)
		if err != nil {
			return nil, err
		}

		tick := &kv.SessionTick{Id: t.Sid, LastTick: time.Now().UnixMicro()}
		return nil, kvOp.Write(tick, tick)
	})
	return e
}

// 使用 fdb 的原则，事务不要超过一个函数
func (t *Transaction) AssignReadXid(sessionid types.SessionId, autoCommit bool) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvOp := NewKvOperator(tr)
		sess := kv.NewSession(t.Sid)
		err := kvOp.Read(sess, sess)
		if err != nil {
			return nil, err
		}
		if sess.State != kv.SessionTransactionStart {
			return nil, errors.New("")
		}
		if sess.ReadTranscationId != 0 {
			return nil, errors.New("")
		}

		maxTid := &kv.MaxTid{Max: 0, DbId: t.Database}
		err = kvOp.Read(maxTid, maxTid)
		if err != nil {
			return nil, err
		}

		sess.ReadTranscationId = maxTid.Max
		err = kvOp.Write(sess, sess)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if e != nil {
		t.Xid = 0
	}
	return e
}

// 使用fdb的原则，事务不要超过一个函数
func (t *Transaction) AssignWriteXid(autoCommit bool) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvOp := NewKvOperator(tr)
		sess := kv.NewSession(t.Sid)
		err := kvOp.Read(sess, sess)
		if err != nil {
			return nil, err
		}
		if sess.State != kv.SessionTransactionStart {
			return nil, errors.New("")
		}
		if sess.ReadTranscationId != 0 {
			return nil, errors.New("")
		}

		maxTid := &kv.MaxTid{Max: 0, DbId: t.Database}
		err = kvOp.Read(maxTid, maxTid)
		if err != nil {
			return nil, err
		}
		maxTid.Max += 1
		err = kvOp.Write(maxTid, maxTid)
		if err != nil {
			return nil, err
		}

		if sess.ReadTranscationId == 0 {
			sess.ReadTranscationId = maxTid.Max
		}
		sess.WriteTranscationId = maxTid.Max
		t.Xid = maxTid.Max

		clog := &kv.TransactionCLog{Sessionid: t.Sid, Tid: t.Xid, DbId: t.Database,
			Status: XS_START}
		err = kvOp.Write(clog, clog)
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if e != nil {
		t.Xid = 0
	}
	return e
}

func (t *Transaction) CheckVaild(tr fdb.Transaction) (error, *kv.TransactionCLog) {
	// 保证 session 是有效的
	kvOp := NewKvOperator(tr)

	var session kv.Session
	session.Id = t.Sid
	err := kvOp.Read(&session, &session)
	if err != nil {
		return err, nil
	}
	if session.State != kv.SessionTransactionStart {
		return errors.New(""), nil
	}
	if t.Xid != InvaildTranscaton && t.Xid != session.WriteTranscationId {
		return errors.New(""), nil
	}
	t.Xid = session.WriteTranscationId
	t.Database = session.DbId

	// 保证事务是有效的
	var clog kv.TransactionCLog
	clog.Tid = t.Xid
	clog.DbId = t.Database

	err = kvOp.Read(&clog, &clog)
	if err != nil {
		return err, nil
	}
	if clog.Status != XS_START {
		return errors.New(""), nil
	}
	return nil, &clog
}

func (t *Transaction) Commit() error {
	// 更新事务状态
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, err = db.Transact(
		func(tr fdb.Transaction) (interface{}, error) {
			err, tkv := t.CheckVaild(tr)
			if err != nil {
				return nil, err
			}
			tkv.Status = XS_COMMIT
			kvOp := NewKvOperator(tr)
			err = kvOp.Write(tkv, tkv)
			if err != nil {
				return nil, err
			}
			return nil, nil
		})
	if err == nil {
		// 异步更新
		// go t.CommitKV(XS_COMMIT)
	}
	return err
}
