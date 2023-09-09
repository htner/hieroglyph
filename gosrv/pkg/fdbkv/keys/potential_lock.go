package keys

import (
	"bytes"
	"encoding/binary"
)

type PotentialLock struct {
	Database uint64
	Relation uint64
	LockType uint8
	Sid      uint64
}

func (K *PotentialLock) Tag() uint16 {
	return PotentialLockTag
}

func (K *PotentialLock) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, K.Database)
	if err != nil {
		return err
	}

	err = binary.Write(buf, binary.LittleEndian, K.Relation)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, K.LockType)
	if err != nil {
		return err
	}

	return binary.Write(buf, binary.LittleEndian, K.Sid)
}

func (K *PotentialLock) RangePerfix(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, K.Database)
	if err != nil {
		return err
	}
  if K.Relation == 0 {
    return nil
  }
	err = binary.Write(buf, binary.LittleEndian, K.Relation)
	if err != nil {
		return err
	}
	if K.LockType == ALLLock {
    return nil
  }
	return binary.Write(buf, binary.LittleEndian, K.LockType)
}

func (K *PotentialLock) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &K.Database)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &K.Relation)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &K.LockType)
	if err != nil {
		return err
	}

	return binary.Read(buf, binary.LittleEndian, &K.Sid)
}
