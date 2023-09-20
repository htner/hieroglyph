package lakehouse

import (
	"errors"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	"github.com/htner/sdb/gosrv/pkg/utils"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

var (
  ErrStateMismatch = errors.New("session mismatch")
)

type SessionOperator struct {
	dbid uint64
	Sid  uint64
	tr   fdb.Transaction
}

func CreateSession(dbid uint64, uid uint64) (*sdb.Session, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	sess := sdb.Session{Id: 0, Uid: uid, Dbid: dbid, State: keys.SessionTransactionIdle}
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		idKey := keys.SecondClassObjectMaxKey{MaxTag: keys.SessionMaxIDTag, Dbid: dbid}
		idOp := utils.NewMaxIdOperator(tr, &idKey)
		sessionid, err := idOp.GetNext()
		if err != nil {
			return nil, err
		}
		sess.Id = sessionid
		sess.State = keys.SessionTransactionIdle
    sess.AutoCommit = true
    sess.QueryId = 0
		op := NewSessionOperator(tr, sess.Id)
		return nil, op.Write(&sess)
	})
	if e != nil {
		return nil, e
	}
	return &sess, e
}

func NewSessionOperator(t fdb.Transaction, sid uint64) *SessionOperator {
	return &SessionOperator{Sid: sid, tr: t}
}

func (s *SessionOperator) Get() (*sdb.Session, error) {
	kvOp := NewKvOperator(s.tr)
	key := &keys.SessionKey{Id: s.Sid}
	var sess sdb.Session
	err := kvOp.ReadPB(key, &sess)
	if err != nil {
		log.Println("not found session", s.Sid)
		return nil, err
	}
	return &sess, nil
}

func (s *SessionOperator) CheckAndGet(state int32) (*sdb.Session, error) {
	kvOp := NewKvOperator(s.tr)
	key := &keys.SessionKey{Id: s.Sid}
	var sess sdb.Session
	err := kvOp.ReadPB(key, &sess)
	if err != nil {
		log.Println("not found session", s.Sid)
		return nil, err
	}
	if sess.State != state {
		log.Println("session states mismatch, ", sess.State, state)
		return &sess, ErrStateMismatch//fmt.Errorf("session mismatch %v %v", sess, stat)
	}
	return &sess, nil
}

func (s *SessionOperator) Write(sess *sdb.Session) error {
	kvOp := NewKvOperator(s.tr)
	key := keys.SessionKey{Id: sess.Id}
	return kvOp.WritePB(&key, sess)
}

func (s *SessionOperator) WriteWithKVOP(op fdbkv.KvOperator, sess *sdb.Session) error {
	key := keys.SessionKey{Id: sess.Id}
	return op.WritePB(&key, sess)
}

func (s *SessionOperator) UnlockAll(tr fdb.Transaction, db uint64) error {
	var mgr LockMgr
	return mgr.UnlockAll(tr, db, s.Sid)
}

func (s *SessionOperator) CommitCurrectTransaction(tr fdb.Transaction, sess *sdb.Session) error {
	transcation := NewTranscation(sess.Dbid, sess.Id)
	return transcation.Commit()
}

func WriteSession(sess *sdb.Session) error {
	// 更新事务状态
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	_, err = db.Transact(
		func(tr fdb.Transaction) (interface{}, error) {
			op := NewSessionOperator(tr, sess.Id)
			return nil, op.Write(sess)
		})
	return err
}
