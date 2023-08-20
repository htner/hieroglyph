package kvpair

import (
	"bytes"
	"encoding/binary"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type ThirdClassObjectMaxKey struct {
	Dbid   types.DatabaseId
	RelId  types.RelId
	MaxTag uint16
}

func (b *ThirdClassObjectMaxKey) Tag() uint16 {
	return b.MaxTag
}

func (m *ThirdClassObjectMaxKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.BigEndian, m.Dbid)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.BigEndian, m.RelId)
}

func (m *ThirdClassObjectMaxKey) DecFdbKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.BigEndian, &m.Dbid)
	if err != nil {
		return err
	}
	return binary.Read(reader, binary.BigEndian, &m.RelId)
}
