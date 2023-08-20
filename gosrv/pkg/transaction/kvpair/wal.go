package kvpair

import (
	"bytes"
	"encoding/binary"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type WAL struct {
	Xid   types.TransactionId
	Seqid uint64
	Log   []byte
}

func NewWAL(xid types.TransactionId, seqid uint64, log []byte) *WAL {
	return &WAL{
		Xid:   xid,
		Seqid: seqid,
		Log:   log,
	}
}

func (w *WAL) Tag() uint16 {
	return WALLogTag
}

func (w *WAL) RangePerfix(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, w.Xid)
}

func (w *WAL) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, w.Xid)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, w.Seqid)
}

func (w *WAL) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, w.Log)
}

func (w *WAL) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &w.Xid)
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, &w.Seqid)
}

func (w *WAL) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &w.Log)
}
