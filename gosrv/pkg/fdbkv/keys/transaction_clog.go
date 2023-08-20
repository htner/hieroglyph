package keys

import (
	"bytes"
	"encoding/binary"
)

type TransactionCLog struct {
	Tid       uint64
	DbId      uint64
	Sessionid uint64
	Status    uint8
}

func (s *TransactionCLog) Tag() uint16 {
	return CLOGTag
}

func (t *TransactionCLog) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, t.DbId)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, t.Tid)
}

func (t *TransactionCLog) EncFdbValue(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, t.Sessionid)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, t.Status)
}

func (t *TransactionCLog) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &t.DbId)
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, &t.Tid)
}

func (t *TransactionCLog) DecFdbValue(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &t.Sessionid)
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, &t.Status)
}
