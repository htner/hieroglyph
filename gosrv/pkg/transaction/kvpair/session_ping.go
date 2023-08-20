package kvpair

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type SessionTick struct {
	Id       types.SessionId
	LastTick int64
}

func NewLocalSessionPing(id types.SessionId) *SessionTick {
	return &SessionTick{
		Id:       id,
		LastTick: time.Now().Local().UnixMicro(),
	}
}

func (s *SessionTick) Tag() uint16 {
	return SessionTickTag
}

func (s *SessionTick) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, s.Id)
}

func (s *SessionTick) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, s.LastTick)
}

func (s *SessionTick) DecFdbKey(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &s.Id)
}

func (s *SessionTick) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &s.LastTick)
}
