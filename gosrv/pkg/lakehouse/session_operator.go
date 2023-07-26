package lakehouse

import (
	"fmt"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/utils"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type SessionOperator struct {
	dbid types.DatabaseId
  Sid types.SessionId
  tr fdb.Transaction
}

func CreateSession(dbid types.DatabaseId, uid uint64) (*kv.Session, error) {
  db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
  sess, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    idKey := kv.SecondClassObjectMaxKey{MaxTag:kv.SessionMaxIDTag, Dbid: dbid}
    idOp := utils.NewMaxIdOperator(tr, &idKey)
    id, err := idOp.GetNext()
    if err != nil {
      return nil, err
    }

    sess := kv.NewLocalSession(types.SessionId(id), uid, dbid, []byte(""))
    sess.State = kv.SessionTransactionIdle
    op := NewSessionOperator(tr, sess.Id)
    return sess, op.Write(sess)
  })
  if e != nil {
    return nil, e
  }
  return sess.(*kv.Session), e 
}

func NewSessionOperator(t fdb.Transaction, sid types.SessionId) *SessionOperator {
  return &SessionOperator{Sid:sid, tr:t}
}

func (s *SessionOperator) CheckAndGet(state int8) (*kv.Session, error) {
	kvOp := NewKvOperator(s.tr)
  sess := kv.NewSession(s.Sid)
  err := kvOp.Read(sess, sess)
  if err != nil {
    log.Println("not found session", s.Sid)
    return nil, err
  }
  if sess.State != state {
    log.Println("session states mismatch")
		return nil, fmt.Errorf("session mismatch %v %v", sess, state)
	}
  return sess, nil
}

func (s *SessionOperator) Write(sess* kv.Session) error {
	kvOp := NewKvOperator(s.tr)
  return kvOp.Write(sess, sess)
}

func (s *SessionOperator) UnlockAll(tr fdb.Transaction, db types.DatabaseId) error {
  var mgr LockMgr
  return mgr.UnlockAll(tr, db, s.Sid, 0)
}

func (s *SessionOperator) CommitCurrectTransaction(tr fdb.Transaction, sess *kv.Session) error {
  transcation := NewTranscationWithXid(sess.DbId, sess.WriteTranscationId, sess.Id)
  return transcation.Commit()
}

func WriteSession(sess* kv.Session) error {
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
    
	
