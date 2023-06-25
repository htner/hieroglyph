package kvpair

import (
	"bytes"
	"encoding/binary"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type TransactionCLog struct {
	Tid       types.TransactionId
	DbId      types.DatabaseId
	Sessionid types.SessionId
	Status    types.XState
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
