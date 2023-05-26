package kvpair

import (
	"bytes"
	"encoding/binary"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type MaxTid struct {
	DbID types.DatabaseId
	Max  types.TransactionId
}

func (m *MaxTid) Tag() uint16 {
	return MAXTIDTag
}

func (m *MaxTid) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, m.DbID)
}

func (m *MaxTid) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, m.Max)
}

func (m *MaxTid) DecFdbKey(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &m.DbID)
}

func (m *MaxTid) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &m.Max)
}
