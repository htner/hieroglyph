package lock

import (
	"errors"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	kv "github.com/htner/sdb/gosrv/pkg/lakehouse/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type LockMgr struct {
}

func (L *LockMgr) Lock(tr fdb.Transaction, lock *Lock) error {
	return L.LockWait(tr, lock, -1)
}

func (L *LockMgr) LockWait(tr fdb.Transaction, lock *Lock, ms int) error {
	perfix, err := kv.MarshalRangePerfix(lock)
	if err != nil {
		return err
	}

	rr := tr.GetRange(fdb.KeyRange{fdb.Key(perfix), fdb.Key{0xFF}}, fdb.RangeOptions{})
	ri := rr.Iterator()

	// Advance will return true until the iterator is exhausted
	for ri.Advance() {
		fdblock := &Lock{}
		data, e := ri.Get()
		if e != nil {
			log.Printf("Unable to read next value: %v\n", e)
			return e
		}
		err := kv.UnmarshalKey(data.Key, fdblock)
		if err != nil {
			return err
		}
		err = kv.UnmarshalValue(data.Value, fdblock)
		if err != nil {
			return err
		}

		// 支持重入
		if fdblock.Xid == lock.Xid && fdblock.LockType == lock.LockType {
			return nil
		}
		if LockConflicts(fdblock.LockType, lock.LockType) {
			fut := tr.Watch(data.Key)
			tr.Commit()
			fut.BlockUntilReady()
			err := fut.Get()
			if err != nil {
				return err
			}
			return errors.New("Retry")
		}
	}
	kvOp := lakehouse.NewKvOperator(tr)
	return kvOp.Write(lock, lock)
}

func (M *LockMgr) Unlock(tr fdb.Transaction, lock *Lock) error {
	kvOp := lakehouse.NewKvOperator(tr)
	return kvOp.Delete(lock)
}

func (M *LockMgr) UnlockAll(tr fdb.Transaction, database types.DatabaseId, xid types.TransactionId) error {
	var lock Lock
	lock.Database = database
	kvOp := lakehouse.NewKvOperator(tr)
	perfix, err := kv.MarshalRangePerfix(&lock)
	if err != nil {
		return err
	}

	rr := tr.GetRange(fdb.KeyRange{Begin: fdb.Key(perfix), End: fdb.Key{0xFF}}, fdb.RangeOptions{})
	ri := rr.Iterator()

	// Advance will return true until the iterator is exhausted
	for ri.Advance() {
		fdblock := &Lock{}
		data, e := ri.Get()
		if e != nil {
			log.Printf("Unable to read next value: %v\n", e)
			return e
		}
		err := kv.UnmarshalKey(data.Key, fdblock)
		if err != nil {
			return err
		}
		err = kv.UnmarshalValue(data.Value, fdblock)
		if err != nil {
			return err
		}

		// 支持重入
		if fdblock.Xid == lock.Xid {
			kvOp.Delete(fdblock)
		}

	}
	return nil
}
