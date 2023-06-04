package kvpair

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type TransactionInfo struct {
	Tid          types.TransactionId
	DbId         types.DatabaseId
	Sessionid    types.SessionId
	Status       uint8
}

func (s *TransactionInfo) Tag() uint16 {
	return TranscationTag
}

func (t *TransactionInfo) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, t.DbId)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, t.Tid)
}

func (t *TransactionInfo) EncFdbValue(buf *bytes.Buffer) error {
	encoder := gob.NewEncoder(buf)
	return encoder.Encode(t)
}

func (t *TransactionInfo) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &t.DbId) 
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, &t.Tid)
}

func (t *TransactionInfo) DecFdbValue(buf *bytes.Reader) error {
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(t)
}
