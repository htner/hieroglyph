package kvpair

import (
	"bytes"
	"encoding/binary"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type TransactionChangeRelKey struct {
	Database types.DatabaseId
	Xid      types.TransactionId
	Rel      types.RelId
}

func (s *TransactionChangeRelKey) Tag() uint16 {
	return TransactionChangeRelKeyTag
}

func (lakeLog *TransactionChangeRelKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, lakeLog.Database)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, lakeLog.Rel)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, lakeLog.Xid)
}

func (lakeLog *TransactionChangeRelKey) DecFdbKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, &lakeLog.Database)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, &lakeLog.Rel)
	if err != nil {
		return err
	}
	return binary.Read(reader, binary.LittleEndian, &lakeLog.Xid)
}
