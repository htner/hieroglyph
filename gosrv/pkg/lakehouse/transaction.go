package transaction

import (
	"errors"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

const (
	XS_NULL   uint8 = 0
	XS_INIT   uint8 = 1
	XS_START  uint8 = 2
	XS_COMMIT uint8 = 3
	XS_ABORT  uint8 = 4
)

const InvaildTranscaton types.TransactionId = 0

type Transaction struct {
	Dbid types.DatabaseId
	Xid  types.TransactionId // write xid
	Sid  types.SessionId
}

func NewTranscation(dbid types.DatabaseId, sid types.SessionId) *Transaction {
	return &Transaction{Xid: InvaildTranscaton,
		Dbid: dbid, Sid: sid}
}

func NewTranscationWithXid(dbid types.DatabaseId, xid types.TransactionId, sid types.SessionId) *Transaction {
	return &Transaction{Xid: xid, Dbid: dbid, Sid: sid}
}

// 使用 fdb 的原则， 启动一个事务
func (t *Transaction) Start(autoCommit bool) error {
	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvOp := NewKvOperator(tr)
		sess := NewSession(t.Sid)
		err := kvOp.Read(sess, sess)
		if err != nil {
			return nil, err
		}
		if sess.State != SessionTransactionIdle {
			return nil, errors.New("session in transaction")
		}
		sess.AutoCommit = autoCommit
		sess.State = SessionTransactionStart
		err = kvOp.kvOp(sess, sess)
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
	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvOp := NewKvOperator(tr)
		sess := NewSession(t.Sid)
		err := kvOp.Read(sess, sess)
		if err != nil {
			return nil, err
		}
		if sess.State != SessionTransactionStart{
			return nil, errors.New("")
		}
		if sess.ReadTranscationId != 0 {
			return nil, errors.New("")
		}

		maxTid := &kv.MaxTid{Max: 0, DbID: t.DbId}
		err = kvOp.Read(maxTid, maxTid)
		if err != nil {
			return nil, err
		}

		st.s.ReadTranscationId = maxTid.Max
		t.Xid = maxTid.Max
		return nil, nil
	})
	if e != nil {
		t.Xid = 0
	}
	return e
}

// 使用fdb的原则，事务不要超过一个函数
func (t *Transaction) AssignWriteXid(autoCommit bool) error {
	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvOp := NewKvOperator(tr)
		sess := NewSession(t.Sid)
		err := kvOp.Read(sess, sess)
		if err != nil {
			return nil, err
		}
		if sess.State != SessionTransactionStart{
			return nil, errors.New("")
		}
		if sess.ReadTranscationId != 0 {
			return nil, errors.New("")
		}

		maxTid := &kv.MaxTid{Max: 0, DbID: t.DbId}
		err = kvOp.Read(maxTid, maxTid)
		if err != nil {
			return nil, err
		}
		maxTid.Max += 1
		err = kvOp.Write(maxTid, maxTid)
		if err != nil {
			return nil, err
		}

		if st.s.ReadTranscationId == 0 {
			st.s.ReadTranscationId = maxTid.Max
		}
		st.s.WriteTranscationId = maxTid.Max
		t.Xid = maxTid.Max

		transactionInfo := &kv.TransactionInfo{Sessionid: t.Sid, Tid: t.Xid, DbId: st.s.DbId,
			Status: XS_START}
		err = kvOp.Write(transactionInfo, transactionInfo)
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

func (t *Transaction) CheckVaild(tr fdb.Transaction) (error, *kv.TransactionInfo) {
	// 保证 session 是有效的
	var session kv.Session
	session.Id = t.Sid
	err = kvOp.Read(&session, &session)
	if err != nil {
		return err, nil
	}
	if session.State != SessionTransactionStart {
		return errors.New(""), nil
	}
	t.Xid = session.WriteTranscationId
	t.Dbid = session.DbId

	// 保证事务是有效的
	var tkv kv.TransactionInfo
	tkv.Tid = t.Xid
	tkv.DbId = t.Dbid
	kvOp := NewKvOperator(tr)
	err := kvOp.Read(&tkv, &tkv)
	if err != nil {
		return err, nil
	}
	if tkv.Status != XS_START {
		return errors.New(""), nil
	}
	return nil, &tkv
}


func (t *Transaction) Commit() error {
	// 更新事务状态
	db := fdb.MustOpenDefault()
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err, tkv := t.CheckVaild(tr)
		if err != nil {
			return nil, err
		}
		tkv.Status = XS_COMMIT
		baseTran := NewKvOperator(tr, tkv, tkv)
		err = baseTran.Write()
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err == nil {
		// 异步更新kv对即可
		go t.CommitKV(XS_COMMIT)
	}
	return err
}
