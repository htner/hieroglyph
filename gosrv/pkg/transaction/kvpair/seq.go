package kvpair

import (
	"bytes"
	"encoding/binary"
)

type Seq struct {
	key Key
	Id  uint64
}

func NewSeq(key Key, id uint64) *Seq {
	return &Seq{
		key: key,
		Id:  id,
	}
}

func (s *Seq) Tag() uint16 {
	return SeqTag
}

func (s *Seq) EncFdbKey(buf *bytes.Buffer) error {
	return s.key.EncKey(buf)
}

func (s *Seq) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, s.Id)
}

func (s *Seq) DecFdbKey(buf *bytes.Reader) error {
	return s.key.DecKey(buf)
}

func (s *Seq) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &s.Id)
}
