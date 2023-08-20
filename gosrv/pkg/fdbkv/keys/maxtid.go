package keys

import (
	"bytes"
	"encoding/binary"
)

type MaxTid struct {
	DbId uint64
	Max  uint64
}

func (m *MaxTid) Tag() uint16 {
	return MAXTIDTag
}

func (m *MaxTid) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, m.DbId)
}

func (m *MaxTid) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, m.Max)
}

func (m *MaxTid) DecFdbKey(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &m.DbId)
}

func (m *MaxTid) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &m.Max)
}
