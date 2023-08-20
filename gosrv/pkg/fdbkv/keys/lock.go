package keys

import (
	"bytes"
	"encoding/binary"
)

type Lock struct {
	Database uint64
	Relation uint64
	LockType uint8
	Sid      uint64
	//Xid      uint64
}

const (
	ALLLock = 0
	// ReadLock   = 1  read not need lock
	// InsertLock = 2  insert not need lock
	UpdateLock = 3 // update / delete
	DDLLock    = 4 // not ddl lock now
)

func GetConflictsLocks(T uint8) []uint8 {
	switch T {
	//case InsertLock:
	//		return []uint8{}
	case UpdateLock:
		return []uint8{UpdateLock}
	case DDLLock:
		//return []uint8{DDLLock, UpdateLock, InsertLock, ReadLock};
		//return []uint8{ALLLock}
		return []uint8{DDLLock}
	}
	return []uint8{}
}

func LockConflicts(T1, T2 uint8) bool {
	if T1 == DDLLock || T2 == DDLLock {
		return true
	}
	if T1 == UpdateLock && T2 == UpdateLock {
		return true
	}
	return false
}

func (K *Lock) Tag() uint16 {
	return LockTag
}

func (K *Lock) EncFdbKey(buf *bytes.Buffer) error {
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

/*
func (V *Lock) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, V.Xid)
}
*/

func (K *Lock) RangePerfix(buf *bytes.Buffer) error {
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

func (K *Lock) DecFdbKey(buf *bytes.Reader) error {
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

/*
func (V *Lock) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &V.Xid)
}
*/
