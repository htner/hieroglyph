package keys

import (
	"bytes"
	"encoding/binary"
)

type QueryKey struct {
	QueryTag  uint16
	Dbid      uint64
	Commandid uint64
}

func NewQueryKey(dbid uint64, commandid uint64, tag uint16) *QueryKey {
	return &QueryKey{QueryTag: tag, Dbid: dbid, Commandid: commandid}
}

func (k *QueryKey) Tag() uint16 {
	return k.QueryTag
}

func (k *QueryKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, k.Dbid)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, k.Commandid)
}

func (k *QueryKey) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &k.Dbid)
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, &k.Commandid)
}

type WorkerTaskKey struct {
	Dbid    uint64
	QueryId uint64
	SliceId int32
	SegId   int32
}

func (k *WorkerTaskKey) Tag() uint16 {
	return WorkerResultTag
}

func (k *WorkerTaskKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, k.Dbid)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, k.QueryId)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, k.SliceId)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, k.SegId)
}

func (k *WorkerTaskKey) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &k.Dbid)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &k.QueryId)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &k.SliceId)
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, &k.SegId)
}
