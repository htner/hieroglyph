package keys

import (
	"bytes"
	"encoding/binary"
)

type FreeWorkerListItem struct {
	ClusterId uint64
	WorkerId  uint64
}

func (L *FreeWorkerListItem) Tag() uint16 {
	return FreeWorkerListItemTag
}

func (L *FreeWorkerListItem) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, L.ClusterId)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, L.WorkerId)
}

func (L *FreeWorkerListItem) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &L.ClusterId)
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, &L.WorkerId)
}

func (L *FreeWorkerListItem) RangePerfix(buf *bytes.Buffer) error {
	return binary.Read(buf, binary.LittleEndian, &L.ClusterId)
}
