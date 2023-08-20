package keys

import (
	"bytes"
	"encoding/binary"
)

type MaxId struct {
	Value uint64
}

func (m *MaxId) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, m.Value)
}

func (m *MaxId) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &m.Value)
}
