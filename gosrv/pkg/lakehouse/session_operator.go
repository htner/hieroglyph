package lakehouse

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type SessionOperator struct {
	Sid      types.SessionId
  tr       fdb.Transaction
}

func NewSessionOperator(t fdb.Transaction, sid types.SessionId) *SessionOperator {
  return &SessionOperator{Sid:sid, tr:t}
}

func (s *SessionOperator) CheckAndGet(state int8) (*kv.Session, error) {
	kvOp := NewKvOperator(s.tr)
  sess := kv.NewSession(s.Sid)
  err := kvOp.Read(sess, sess)
  if err != nil {
    return nil, err
  }
  if sess.State != state {
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
