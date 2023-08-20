package keys

import (
	"bytes"
	"encoding/binary"

	"github.com/google/uuid"
)

const (
	InsertMark    int8 = 1
	DeleteMark    int8 = 2
	PreInsertMark int8 = 3
)

type LakeLogItemKey struct {
	Database uint64
	Rel      uint64
	Xid      uint64
	Action   int8
	UUID     string
}

func NewLakeLogItemKey(database uint64, rel uint64, xid uint64, action int8) *LakeLogItemKey {
	return &LakeLogItemKey{
		Database: database,
		Rel:      rel,
		Xid:      xid,
		Action:   action,
		UUID:     uuid.New().String(),
	}
}

func (s *LakeLogItemKey) Tag() uint16 {
	return LakeLogItemKeyTag
}

func (item *LakeLogItemKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, item.Database)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, item.Rel)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, item.Xid)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, item.Action)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(item.UUID)
	return err
}

func (item *LakeLogItemKey) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &item.Database)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &item.Rel)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &item.Xid)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &item.Action)
	if err != nil {
		return err
	}
	b := make([]byte, 0)
	_, err = buf.Read(b)
	if err != nil {
		return err
	}
	item.UUID = string(b)
	return nil
}
