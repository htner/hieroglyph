package kvpair

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type Session struct {
	Id   types.SessionId
	Uid  uint64
	DbId types.DatabaseId

	Token               []byte
	InTransaction       bool
	InManualTransaction bool

	ReadTranscationId    types.TransactionId
	WriteTranscationId   types.TransactionId
	ReadOnlyTransactions map[types.DatabaseId]types.TransactionId
}

func NewLocalSession(id types.SessionId, uid uint64, dbid types.DatabaseId, token []byte) *Session {
	return &Session{
		Id:   id,
		Uid:  uid,
		DbId: dbid,

		Token: token,

		ReadTranscationId:   0,
		WriteTranscationId:  0,
		InManualTransaction: false,
		InTransaction:       false,
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
