package lakehouse

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	"github.com/htner/sdb/gosrv/proto/sdb"
	log "github.com/sirupsen/logrus"
)

const (
	XS_NULL   uint8 = uint8(0)
	XS_INIT   uint8 = uint8(1)
	XS_START  uint8 = uint8(2)
	XS_COMMIT uint8 = uint8(3)
	XS_ABORT  uint8 = uint8(4)
)

const InvaildTranscaton uint64 = 0

type Transaction struct {
	Database uint64
	Sid      uint64
	session  *sdb.Session
	clog     *keys.TransactionCLog
}

func NewTranscation(dbid uint64, sid uint64) *Transaction {
	return &Transaction{Database: dbid, Sid: sid}
}

func (t *Transaction) GetSession() *sdb.Session {
	return t.session
}

// 使用 fdb 的原则， 启动一个事务
func (t *Transaction) Start(autoCommit bool) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		sessOp := NewSessionOperator(tr, t.Sid)
		t.session, err = sessOp.CheckAndGet(keys.SessionTransactionIdle)
		if err != nil {
			return nil, err
		}

		t.session.AutoCommit = autoCommit
		t.session.State = keys.SessionTransactionStart
		return nil, sessOp.Write(t.session)
		/*
			err = kvOp.Write(sess, sess)
			if err != nil {
				return nil, err
			}
		*/

		//tick := &keys.SessionTick{Id: t.Sid, LastTick: time.Now().UnixMicro()}
		//return nil, kvOp.Write(tick, tick)
	})
	return e
}

// 使用 fdb 的原则，事务不要超过一个函数
func (t *Transaction) CheckReadAble(tr fdb.Transaction) error {
	kvOp := NewKvOperator(tr)

	sessOp := NewSessionOperator(tr, t.Sid)
	var err error
	t.session, err = sessOp.CheckAndGet(keys.SessionTransactionStart)
	if err != nil {
		log.Printf("check and get error")
		return err
	}

	if t.session.ReadTransactionId != 0 {
		return nil
	}

	maxTid := &keys.MaxTid{Max: 0, DbId: t.Database}
	err = kvOp.Read(maxTid, maxTid)
	if err != nil {
		if err != fdbkv.ErrEmptyData {
			log.Printf("read max tid error")
			return err
		}
	}

	t.session.ReadTransactionId = maxTid.Max
	err = sessOp.Write(t.session)
	if err != nil {
		log.Printf("write sess error")
		return err
	}
	return nil
}

func (t *Transaction) CheckWriteAble(tr fdb.Transaction) error {
	// 保证 session 是有效的
	kvOp := NewKvOperator(tr)

	var err error
	sessOp := NewSessionOperator(tr, t.Sid)
	t.session, err = sessOp.CheckAndGet(keys.SessionTransactionStart)
	if err != nil {
		return err
	}

	if t.session.WriteTransactionId != InvaildTranscaton {
		return nil
	}

	maxTid := &keys.MaxTid{Max: 0, DbId: t.Database}
	err = kvOp.Read(maxTid, maxTid)
	if err != nil {
		if err != fdbkv.ErrEmptyData {
			log.Printf("read max tid error")
			return err
		}
	}
	maxTid.Max += 1
	err = kvOp.Write(maxTid, maxTid)
	if err != nil {
		return err
	}
	t.session.WriteTransactionId = maxTid.Max
	if t.session.ReadTransactionId == InvaildTranscaton {
		t.session.ReadTransactionId = maxTid.Max
	}

	clog := &keys.TransactionCLog{Sessionid: t.Sid, Tid: maxTid.Max, DbId: t.Database, Status: XS_START}
	err = kvOp.Write(clog, clog)
	if err != nil {
		return err
	}
	return sessOp.Write(t.session)
}

func (t *Transaction) CheckVaild(tr fdb.Transaction) error {
	// 保证 session 是有效的
	kvOp := NewKvOperator(tr)

	sessOp := NewSessionOperator(tr, t.Sid)
	var err error
	t.session, err = sessOp.CheckAndGet(keys.SessionTransactionStart)
	if err != nil {
		return err
	}

	if t.session.WriteTransactionId == InvaildTranscaton {
		return nil
	}
	t.Database = t.session.Dbid

	// 保证事务是有效的
	var clog keys.TransactionCLog
	clog.Tid = t.session.WriteTransactionId
	clog.DbId = t.Database

	err = kvOp.Read(&clog, &clog)
	if err != nil {
		return err
	}
	if clog.Status != XS_START {
		return errors.New("session not start")
	}
	t.clog = &clog
	return nil
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
			err := t.CheckVaild(tr)
			if err != nil {
				return nil, err
			}
			if t.clog == nil {
				return nil, nil
			}

			t.clog.Status = XS_COMMIT
			kvOp := NewKvOperator(tr)
			err = kvOp.Write(t.clog, t.clog)
			if err != nil {
				return nil, err
			}

			t.session.AutoCommit = false
			t.session.ReadTransactionId = InvaildTranscaton
			t.session.WriteTransactionId = InvaildTranscaton
			t.session.State = keys.SessionTransactionIdle

			log.Printf("reset session")
			err = sessOp.Write(t.session)
			if err != nil {
				return nil, err
			}

			var mgr LockMgr
			return nil, mgr.UnlockAll(tr, t.Database, t.session.Id)
		})
	return err
}

func (t *Transaction) WriteAble() error {
	// 更新事务状态
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, err = db.Transact(
		func(tr fdb.Transaction) (interface{}, error) {
			return nil, t.CheckWriteAble(tr)
		})
	// 异步更新
	// go t.CommitKV(XS_COMMIT)
	return err
}

func (t *Transaction) ReadAble() error {
	// 更新事务状态
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, err = db.Transact(
		func(tr fdb.Transaction) (interface{}, error) {
			return nil, t.CheckReadAble(tr)
		})
	// 异步更新
	// go t.CommitKV(XS_COMMIT)
	return err
}

func (t *Transaction) State(kvReader *fdbkv.KvReader, xid uint64) (uint8, error) {
	/*
		if xid == 1 {
			return XS_COMMIT, nil
		}
	*/
	var minClog keys.TransactionCLog
	minClog.Tid = uint64(xid)
	minClog.DbId = t.Database
	err := kvReader.Read(&minClog, &minClog)
	if err != nil {
		return XS_NULL, err
	}
	return minClog.Status, nil
}

/*
func (t *Transaction) OpState(kvReader *fdbkv.KvOperator, xid uint64) (uint8, error) {
	var minClog keys.TransactionCLog
	minClog.Tid = uint64(xid)
	minClog.DbId = t.Database
	err := kvReader.Read(&minClog, &minClog)
	if err != nil {
		return XS_NULL, errors.New("read error")
	}
	return minClog.Status, nil
}
*/

func (t *Transaction) TryAutoCommit() error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	isAutoCommit, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		sessOp := NewSessionOperator(tr, t.Sid)
		t.session, err = sessOp.CheckAndGet(keys.SessionTransactionIdle)
		if err != nil {
			return false, err
		}
		return t.session.AutoCommit, nil
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
