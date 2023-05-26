package kvpair

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type LockKey struct {
	LocktagField1 uint64
	LocktagField2 uint64
	LocktagField3 uint64
	LockType      uint8
	Sid           types.SessionId
}

const (
	ReadLock   = 1
	InsertLock = 2
	UpdateLock = 3 // update / delete
	DDLLock    = 4
)

type LockValue struct {
	Xid types.TransactionId
}

type WaittingLockKey struct {
	LockKey
	TimeUS uint64 //  同一个锁，按时间排序
	Xid    uint64
}

// 按xid来查找
type TransactionLockKey struct {
	Xid           types.TransactionId
	LockKeyDetail LockKey
}

type TransactionLockValue struct {
	LockStats uint8 // waitting or lock
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

func (K *LockKey) Tag() uint16 {
	return LockTag
}

func (K *LockKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, K.LocktagField1)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, K.LocktagField2)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, K.LocktagField3)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, K.LockType)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, K.Sid)
}

func (V *LockValue) EncFdbValue(buf *bytes.Buffer) error {
	encoder := gob.NewEncoder(buf)
	return encoder.Encode(V)
}

func (K *LockKey) RangePerfix(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, K.LocktagField1)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, K.LocktagField2)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, K.LocktagField3)
}

func (K *LockKey) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &K.LocktagField1)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, K.LocktagField2)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, K.LocktagField3)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, K.LockType)
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, K.Sid)
}

func (V *LockValue) DecFdbValue(buf *bytes.Reader) error {
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(V)
}
