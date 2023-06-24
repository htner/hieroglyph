package kvpair

import (
	"bytes"
	"encoding/binary"
)

type CommKey struct {
	Cid uint64
}

func (s *CommKey) Tag() uint16 {
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
