package lock

import (
	"bytes"
	"encoding/binary"

	kv "github.com/htner/sdb/gosrv/pkg/lakehouse/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type Lock struct {
	Database types.DatabaseId
	Relation types.RelId
	LockType uint8
	Sid      types.SessionId
	Xid      types.TransactionId
}

const (
	ReadLock   = 1
	InsertLock = 2
	UpdateLock = 3 // update / delete
	DDLLock    = 4
)

type WaittingLockKey struct {
	Lock
	TimeUS uint64 //  同一个锁，按时间排序
	Xid    uint64
}

// 按xid来查找
type TransactionLockKey struct {
	Xid types.TransactionId
	//LockKeyDetail LockKey
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

func (K *Lock) Tag() uint16 {
	return kv.LockTag
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

func (V *Lock) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, V.Xid)
}

func (K *Lock) RangePerfix(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, K.Database)
}

func (K *Lock) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &K.Database)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, K.Relation)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, K.LockType)
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, K.Sid)
}

func (V *Lock) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, V.Xid)
}
