package kvpair

import (
	"bytes"
	"encoding/binary"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type MaxFileID struct {
	DbId types.DatabaseId
	Rel types.RelId
	Max uint64 
}

func (m *MaxFileID) Tag() uint16 {
	return MAXFILEIDTag
}

func (m *MaxFileID) EncFdbKey(buf *bytes.Buffer) error {
  err := binary.Write(buf, binary.LittleEndian, m.DbId)
  if err != nil {
    return err
  }
  return binary.Write(buf, binary.LittleEndian, m.Rel)
}

func (m *MaxFileID) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, m.Max)
}

func (m *MaxFileID) DecFdbKey(buf *bytes.Reader) error {
  err := binary.Read(buf, binary.LittleEndian, &m.DbId)
  if err != nil {
    return err
  }
  return binary.Read(buf, binary.LittleEndian, &m.Rel)
}

func (m *MaxFileID) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &m.Max)
}
