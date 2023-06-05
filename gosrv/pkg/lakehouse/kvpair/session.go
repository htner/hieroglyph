package kvpair

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/htner/sdb/gosrv/pkg/types"
)

const (
	SessionTransactionInvaild int8 = -1
	SessionTransactionIdle    int8 = 1
	SessionTransactionStart   int8 = 2
	SessionTransactionCommit  int8 = 3
)

type Session struct {
	Id   types.SessionId
	Uid  uint64
	DbId types.DatabaseId

	Token      []byte
	State      int8
	AutoCommit bool

	ReadTranscationId    types.TransactionId
	WriteTranscationId   types.TransactionId
	ReadOnlyTransactions map[types.DatabaseId]types.TransactionId
}

func NewSession(id types.SessionId) *Session {
	return &Session{
		Id:                 id,
		Uid:                0,
		DbId:               0,
		ReadTranscationId:  0,
		WriteTranscationId: 0,
		State:              SessionTransactionInvaild,
		AutoCommit:         false,
	}
}

func NewLocalSession(id types.SessionId, uid uint64, dbid types.DatabaseId, token []byte) *Session {
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

func (s *Session) Tag() uint16 {
	return SessionTag
}

func (s *Session) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, s.Id)
}

func (s *Session) EncFdbValue(buf *bytes.Buffer) error {
	encoder := gob.NewEncoder(buf)
	return encoder.Encode(s)
}

func (s *Session) DecFdbKey(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &s.Id)
}

func (s *Session) DecFdbValue(buf *bytes.Reader) error {
	decoder := gob.NewDecoder(buf)
	err := decoder.Decode(s)
	return err
}
