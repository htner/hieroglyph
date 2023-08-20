package keys

import (
	"bytes"
	"encoding/binary"
	"time"
)

type SessionTick struct {
	Id       uint64
	LastTick int64
}

func NewLocalSessionPing(id uint64) *SessionTick {
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
