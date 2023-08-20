package keys

import (
	"bytes"
	"encoding/binary"
)

type LakeLogKey struct {
	database uint64
	rel      uint64
	xid      uint64
}

func (s *LakeLogKey) Tag() uint16 {
	return LakeLogKeyTag
}

func (lakeLog *LakeLogKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, lakeLog.database)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, lakeLog.rel)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, lakeLog.xid)
}

func (lakeLog *LakeLogKey) DecFdbKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, &lakeLog.database)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, &lakeLog.rel)
	if err != nil {
		return err
	}
	return binary.Read(reader, binary.LittleEndian, &lakeLog.xid)
}
