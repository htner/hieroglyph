package lakehouse

import (
	"errors"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/lakehouse/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

var ErrorRetry = errors.New("retry")

type LockMgr struct {
}

func (L *LockMgr) DoWithLock(db fdb.Database, lock *Lock, f func(fdb.Transaction) (interface{}, error), retryNum int) (data interface{}, err error) {
	//return L.LockWait(tr, lock, -1)
	for i := 0; i < retryNum; i++ {
		data, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			err := L.LockWait(tr, lock, -1)
			if err != nil {
				return nil, err
			}
			return f(tr)
		})
		if err != nil {
			return data, err
		}
	}
	return data, err
}

func (L *LockMgr) Lock(tr fdb.Transaction, lock *Lock) error {
	return L.LockWait(tr, lock, -1)
}

func (L *LockMgr) LockWait(tr fdb.Transaction, lock *Lock, ms int) error {
	perfix, err := kv.MarshalRangePerfix(lock)
	if err != nil {
		return err
	}

	rr := tr.GetRange(fdb.KeyRange{Begin: fdb.Key(perfix), End: fdb.Key{0xFF}},
		fdb.RangeOptions{})
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
	kvOp := NewKvOperator(tr)
	return kvOp.Write(lock, lock)
}

func (M *LockMgr) Unlock(tr fdb.Transaction, lock *Lock) error {
	kvOp := NewKvOperator(tr)
	return kvOp.Delete(lock)
}

func (M *LockMgr) UnlockAll(tr fdb.Transaction, database types.DatabaseId, xid types.TransactionId) error {
	var lock Lock
	lock.Database = database
	kvOp := NewKvOperator(tr)
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
