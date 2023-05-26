package transaction

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type SessionOperator struct {
	t fdb.Transaction
	s *kv.Session
}

func NewSessionOperator(tr fdb.Transaction, sess *kv.Session) *SessionOperator {
	return &SessionOperator{t: tr, s: sess}
}

func (s *SessionOperator) RegisterSession() error {
	sKey, err := kv.MarshalKey(s.s)
	fKey := fdb.Key(sKey)
	if err != nil {
		return err
	}
	future := s.t.Get(fKey)

	_, e := future.Get()
	if e != nil {
		return errors.New("session id confilct")
	}
	sValue, err := kv.MarshalValue(s.s)
	if err != nil {
		return err
	}
	s.t.Set(fKey, sValue)
	return nil
}

func (s *SessionOperator) UpdateSession() error {
	sKey, err := kv.MarshalKey(s.s)
	fKey := fdb.Key(sKey)
	if err != nil {
		return err
	}
	sValue, err := kv.MarshalValue(s.s)
	if err != nil {
		return err
	}
	s.t.Set(fKey, sValue)
	return nil
}

func (s *SessionOperator) GetSession(sessionid types.SessionId) error {
	s.s = kv.NewLocalSession(sessionid, 0, 0, nil)
	sKey, err := kv.MarshalKey(s.s)
	fKey := fdb.Key(sKey)
	if err != nil {
		return err
	}
	future := s.t.Get(fKey)
	value, e := future.Get()
	if e == nil {
		return errors.New("session id no found")
	}
	err = kv.UnmarshalValue(value, s.s)
	if e == nil {
		return errors.New("session value error")
	}
	if s.s.Id != sessionid {
		return errors.New("session error")
	}
	return nil
}

func (s *SessionOperator) CheckSessionAlive(sessionid types.SessionId) bool {
	return s.GetSession(sessionid) == nil
}
