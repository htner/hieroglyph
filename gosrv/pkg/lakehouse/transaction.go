package lakehouse

import (
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	kv "github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	log "github.com/sirupsen/logrus"
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
		sessOp := NewSessionOperator(tr, t.Sid)
		sess, err := sessOp.CheckAndGet(kv.SessionTransactionIdle)
		if err != nil {
			return nil, err
		}

		sess.AutoCommit = autoCommit
		sess.State = kv.SessionTransactionStart
		err = kvOp.Write(sess, sess)
		if err != nil {
			return nil, err
		}

		//tick := &kv.SessionTick{Id: t.Sid, LastTick: time.Now().UnixMicro()}
		//return nil, kvOp.Write(tick, tick)
		return nil, nil
	})
	return e
}

// 使用 fdb 的原则，事务不要超过一个函数
func (t *Transaction) CheckReadAble(tr fdb.Transaction) (*kv.Session, error) {
	kvOp := NewKvOperator(tr)

	sessOp := NewSessionOperator(tr, t.Sid)
	sess, err := sessOp.CheckAndGet(kv.SessionTransactionStart)
	if err != nil {
		log.Printf("check and get error")
		return nil, err
	}

	if sess.ReadTranscationId != 0 {
		return sess, nil
	}

	maxTid := &kv.MaxTid{Max: 0, DbId: t.Database}
	err = kvOp.Read(maxTid, maxTid)
	if err != nil {
		if err != fdbkv.ErrEmptyData {
			log.Printf("read max tid error")
			return nil, err
		}
	}

	sess.ReadTranscationId = maxTid.Max
	err = kvOp.Write(sess, sess)
	if err != nil {
		log.Printf("write sess error")
		return nil, err
	}
	return sess, nil
}

func (t *Transaction) CheckWriteAble(tr fdb.Transaction) (*kv.Session, error) {
	// 保证 session 是有效的
	kvOp := NewKvOperator(tr)

	sessOp := NewSessionOperator(tr, t.Sid)
	session, err := sessOp.CheckAndGet(kv.SessionTransactionStart)
	if err != nil {
		return nil, err
	}

	if t.Xid != InvaildTranscaton && t.Xid != session.WriteTranscationId {
		return nil, fmt.Errorf("t.xid error %v-%v", t.Xid, session.WriteTranscationId)
	}

	if session.WriteTranscationId != InvaildTranscaton {
		// reset to write transact id
		t.Xid = session.WriteTranscationId
		return session, nil
	}

	maxTid := &kv.MaxTid{Max: 0, DbId: t.Database}
	err = kvOp.Read(maxTid, maxTid)
	if err != nil {
		// TODO check noexist
		// return &session, err
	}
	maxTid.Max += 1
	err = kvOp.Write(maxTid, maxTid)
	if err != nil {
		return session, err
	}
	session.WriteTranscationId = maxTid.Max
	if session.ReadTranscationId == InvaildTranscaton {
		session.ReadTranscationId = maxTid.Max
	}
	t.Xid = maxTid.Max

	clog := &kv.TransactionCLog{Sessionid: t.Sid, Tid: t.Xid, DbId: t.Database,
		Status: XS_START}
	err = kvOp.Write(clog, clog)
	if err != nil {
		return session, err
	}
	return session, kvOp.Write(session, session)
}

func (t *Transaction) CheckVaild(tr fdb.Transaction) (*kv.TransactionCLog, *kv.Session, error) {
	// 保证 session 是有效的
	kvOp := NewKvOperator(tr)

	sessOp := NewSessionOperator(tr, t.Sid)
	session, err := sessOp.CheckAndGet(kv.SessionTransactionStart)
	if err != nil {
		return nil, nil, err
	}

	if t.Xid != InvaildTranscaton && t.Xid != session.WriteTranscationId {
		return nil, nil, errors.New("xid error")
	}

	if session.WriteTranscationId == InvaildTranscaton {
		return nil, nil, nil
	}
	t.Xid = session.WriteTranscationId
	t.Database = session.DbId

	// 保证事务是有效的
	var clog kv.TransactionCLog
	clog.Tid = t.Xid
	clog.DbId = t.Database

	err = kvOp.Read(&clog, &clog)
	if err != nil {
		return nil, nil, err
	}
	if clog.Status != XS_START {
		return nil, nil, errors.New("session not start")
	}
	return &clog, session, nil
}

func (t *Transaction) Commit() error {
	// 更新事务状态
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, err = db.Transact(
		func(tr fdb.Transaction) (interface{}, error) {
			sessOp := NewSessionOperator(tr, t.Sid)
			tkv, session, err := t.CheckVaild(tr)
			if err != nil {
				return nil, err
			}
			if tkv == nil {
				return nil, nil
			}

			tkv.Status = XS_COMMIT
			kvOp := NewKvOperator(tr)
			err = kvOp.Write(tkv, tkv)
			if err != nil {
				return nil, err
			}

			session.AutoCommit = false
			session.ReadTranscationId = InvaildTranscaton
			session.WriteTranscationId = InvaildTranscaton
			session.State = kv.SessionTransactionIdle

			log.Printf("reset session")
			return nil, sessOp.Write(session)
		})
	if err == nil {
		_, err = db.Transact(
			func(tr fdb.Transaction) (interface{}, error) {
				var mgr LockMgr
				err := mgr.UnlockAll(tr, t.Database, t.Sid, t.Xid)
				return nil, err
			})
		// 异步更新
		// go t.CommitKV(XS_COMMIT)
	}
	return err
}

func (t *Transaction) WriteAble() (*kv.Session, error) {
	// 更新事务状态
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	sess, err := db.Transact(
		func(tr fdb.Transaction) (interface{}, error) {
			return t.CheckWriteAble(tr)
		})
	// 异步更新
	// go t.CommitKV(XS_COMMIT)
	return sess.(*kv.Session), err
}

func (t *Transaction) ReadAble() (*kv.Session, error) {
	// 更新事务状态
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	sess, err := db.Transact(
		func(tr fdb.Transaction) (interface{}, error) {
			return t.CheckReadAble(tr)
		})
	// 异步更新
	// go t.CommitKV(XS_COMMIT)
	return sess.(*kv.Session), err
}

func (t *Transaction) State(kvReader *fdbkv.KvReader, xid types.TransactionId) (types.XState, error) {
	if t.Xid == 1 {
		return XS_COMMIT, nil
	}
	var minClog kv.TransactionCLog
	minClog.Tid = types.TransactionId(xid)
	minClog.DbId = t.Database
	err := kvReader.Read(&minClog, &minClog)
	if err != nil {
		return XS_NULL, err
	}
	return minClog.Status, nil
}

func (t *Transaction) OpState(kvReader *fdbkv.KvOperator, xid types.TransactionId) (types.XState, error) {
	var minClog kv.TransactionCLog
	minClog.Tid = types.TransactionId(xid)
	minClog.DbId = t.Database
	err := kvReader.Read(&minClog, &minClog)
	if err != nil {
		return XS_NULL, errors.New("read error")
	}
	return minClog.Status, nil
}

func (t *Transaction) TryAutoCommit() error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	isAutoCommit, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		sessOp := NewSessionOperator(tr, t.Sid)
		sess, err := sessOp.CheckAndGet(kv.SessionTransactionIdle)
		if err != nil {
			return false, err
		}
		return sess.AutoCommit, nil
	})

	if err != nil {
		return err
	}

	if isAutoCommit.(bool) {
		log.Printf("auto commit")
		return t.Commit()
	}
	return nil
}
