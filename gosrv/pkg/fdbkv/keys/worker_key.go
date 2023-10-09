package keys

import (
	"bytes"
	"encoding/binary"
)

type WorkerStatusKey struct {
	Cluster uint32
	WorkerId uint64
}

func (*WorkerStatusKey) Tag() uint16 {
	return WorkerStatusTag 
}

func (worker *WorkerStatusKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, worker.Cluster)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.BigEndian, worker.WorkerId)
}

func (worker *WorkerStatusKey) DecFdbKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, &worker.Cluster)
	if err != nil {
		return err
	}
	return binary.Read(reader, binary.BigEndian, &worker.WorkerId)
}

func (worker *WorkerStatusKey) RangePerfix(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, worker.Cluster)
}
