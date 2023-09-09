package keys

import (
	"bytes"
	"encoding/binary"
)

const (
	SessionTransactionInvaild int32 = -1
	SessionTransactionIdle    int32 = 2
	SessionTransactionStart   int32 = 3
	SessionTransactionCommit  int32 = 4
)

type SessionKey struct {
	Id uint64
}

func NewSessionKey(id uint64) *SessionKey {
	return &SessionKey{
		Id: id,
	}
}

/*
func NewLocalSession(id uint64) *Session {
	return &Session{
		Id:                 id,
		Uid:                uid,
		DbId:               dbid,
		Token:              token,
		ReadTranscationId:  0,
		WriteTranscationId: 0,
		State:              SessionTransactionInvaild,
		AutoCommit:         false,
	}
}
*/

func (s *SessionKey) Tag() uint16 {
	return SessionTag
}

func (s *SessionKey) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, s.Id)
}

func (s *SessionKey) DecFdbKey(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &s.Id)
}
