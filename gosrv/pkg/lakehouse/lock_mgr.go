package lakehouse

import (
	"errors"
	"fmt"
	"log"
  "reflect"
  "runtime"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

var ErrorRetry = errors.New("retry")

type LockMgr struct {
}

func GetFunctionName(i interface{}) string {
    return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func (L *LockMgr) DoWithAutoLock(db fdb.Database, lock *Lock, f func(fdb.Transaction) (interface{}, error), retryNum int) (data interface{}, err error) {
	//return L.LockWait(tr, lock, -1)
	for i := 0; i < retryNum; i++ {
		data, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			err := L.TryLockAndWatch(tr, lock)
			fmt.Println(db, tr, lock, i)
			if i != 0 {
				log.Printf("do with auto lock retry %d", i)
			}
			return nil, err
		})
    if err == nil {
      break
    }
		fmt.Printf("lock and watch error%v", err)
		//fmt.Printf("xxx %v", err)
	}

	if err == nil {
		//fmt.Printf("xxx %v", err)
		data, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			return f(tr)
		})
		if err != nil {
      fmt.Printf("do function %s error : %v", GetFunctionName(f), err)
			return nil, err
		}
		//_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
	  //	return nil, L.Unlock(tr, lock)
		//})
	}
	return data, err
}

func (L *LockMgr) Lock(tr fdb.Transaction, lock *Lock) error {
	return L.TryLockAndWatch(tr, lock)
}

func (L *LockMgr) TryCheckConflicts(tr fdb.Transaction, checkLock *Lock, realType uint8) (lockBefore bool, err error) {
	prefix, err := kv.MarshalRangePerfix(checkLock)
	if err != nil {
		return false, err
	}
	pr, err := fdb.PrefixRange(prefix)
	if err != nil {
		return false, err
	}
  log.Println("check lock:", checkLock)
	//log.Println(pr)
	// Read and process the range
	kvs, err := tr.GetRange(pr, fdb.RangeOptions{}).GetSliceWithError()
	if err != nil {
		return false, err
	}
	//log.Println(prefix, kvs)

	// Advance will return true until the iterator is exhausted
	for _, data := range kvs {
		fdblock := &Lock{}
		err := kv.UnmarshalKey(data.Key, fdblock)
		if err != nil {
			log.Printf("Unable to UnmarshalKey: %v %v\n", data, err)
			return false, err
		}
		err = kv.UnmarshalValue(data.Value, fdblock)
		if err != nil {
			log.Printf("Unable to UnmarshalValue: %v %v\n", data, err)
			return false, err
		}

		 log.Printf("get other lock: %v\n", fdblock)
		// 支持重入
		if fdblock.Sid == checkLock.Sid && fdblock.LockType == realType {
			log.Printf("reentry %v\n", checkLock)
			return true, nil
		}
		if LockConflicts(fdblock.LockType, realType) {
			fut := tr.Watch(data.Key)
			log.Printf("LockConflicts Watch: %s\n", data.Key)
			tr.Commit()
			fut.BlockUntilReady()
			err := fut.Get()
			if err != nil {
				return false, err
			}
			log.Printf("Retry, LockConflicts key: %s\n", data.Key)
			return false, errors.New("Retry")
		}
	}
	return false, nil
}

func (L *LockMgr) TryLockAndWatch(tr fdb.Transaction, lock *Lock) error {
	sessOp := NewSessionOperator(tr, lock.Sid)
	sess, err := sessOp.CheckAndGet(kv.SessionTransactionStart)
	if err != nil {
		log.Printf("CheckAndGet error %v %v %v %v", err, L, sessOp, lock)
		return err
	}
	log.Printf("lock in session %v", sess)

	var checkLock Lock
	checkLock.Database = lock.Database
	checkLock.Relation = lock.Relation
  checkLock.Sid = lock.Sid
	conflictsTypes := GetConflictsLocks(lock.LockType)

	for _, conflictsType := range conflictsTypes {
		checkLock.LockType = conflictsType
		// log.Println(checkLock, conflictsTypes)
		lockBefore, err := L.TryCheckConflicts(tr, &checkLock, lock.LockType)
		if err != nil {
			return err
		}
		if lockBefore {
			log.Printf("lockBefore, do not need write")
			return nil
		}
	}

	kvOp := NewKvOperator(tr)
	return kvOp.Write(lock, lock)
}

func (M *LockMgr) Unlock(tr fdb.Transaction, lock *Lock) error {
	kvOp := NewKvOperator(tr)
	return kvOp.Delete(lock)
}

func (M *LockMgr) UnlockAll(tr fdb.Transaction, database types.DatabaseId, sid types.SessionId, xid types.TransactionId) error {
	var lock Lock
	lock.Database = database
	kvOp := NewKvOperator(tr)
	prefix, err := kv.MarshalRangePerfix(&lock)
	if err != nil {
		return err
	}

	pr, err := fdb.PrefixRange(prefix)
	if err != nil {
		return err
	}
	// Read and process the range
	kvs, err := tr.GetRange(pr, fdb.RangeOptions{}).GetSliceWithError()
	if err != nil {
		return err
	}

	// Advance will return true until the iterator is exhausted
	for _, data := range kvs {
		fdblock := &Lock{}
		err := kv.UnmarshalKey(data.Key, fdblock)
		if err != nil {
			return err
		}
		err = kv.UnmarshalValue(data.Value, fdblock)
		if err != nil {
			return err
		}

		if fdblock.Sid == sid {
			if fdblock.Xid == xid || xid == InvaildTranscaton {
				kvOp.Delete(fdblock)
			}
		}
	}
	return nil
}
