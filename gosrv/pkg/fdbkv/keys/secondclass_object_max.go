package keys

import (
	"bytes"
	"encoding/binary"
)

type SecondClassObjectMaxKey struct {
	Dbid   uint64
	MaxTag uint16
}

func (b *SecondClassObjectMaxKey) Tag() uint16 {
	return b.MaxTag
}

func (m *SecondClassObjectMaxKey) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.BigEndian, m.Dbid)
}

func (m *SecondClassObjectMaxKey) DecFdbKey(reader *bytes.Reader) error {
	return binary.Read(reader, binary.BigEndian, &m.Dbid)
}
