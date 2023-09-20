package lakehouse

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	log "github.com/sirupsen/logrus"
)

var ErrorRetry = errors.New("retry")
// var ErrorRetry = errors.New("retry")

type LockMgr struct {
}

func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func (L *LockMgr) TryLock(dbid uint64, sid uint64, rel uint64) error {
  db, err := fdb.OpenDefault()
  if err != nil {
    return err
  }

  var fdblock keys.Lock
  fdblock.Database = dbid
  fdblock.Relation = rel
  fdblock.LockType = keys.UpdateLock
  fdblock.Sid = sid

  // log.Println("pre lock", fdblock)

  retryNum := 10
  for i := 0; i < retryNum; i++ {
    _, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
      e := L.Lock(tr, &fdblock)
      if e != nil {
        return nil, e
      }
      return nil, nil
    })

    if err == ErrorRetry {
      log.Printf("pre lock and retry %d", i)
      continue
    } else {
      break
    }
  }
  return err
}

func (L *LockMgr) TryUnlock(dbid uint64, sid uint64, rel uint64) error {
  db, err := fdb.OpenDefault()
  if err != nil {
    return err
  }

  var fdblock keys.Lock
  fdblock.Database = dbid
  fdblock.Relation = rel
  fdblock.LockType = keys.UpdateLock
  fdblock.Sid = sid

  _, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    // log.Println("unlock", fdblock)
    return nil, L.Unlock(tr, &fdblock)
  })
  return err
}

func (L *LockMgr) PreLock(dbid uint64, sid uint64, updates []uint64) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
  }

  for _, rel := range updates {

    var fdblock keys.Lock
    fdblock.Database = dbid
    fdblock.Relation = rel
    fdblock.LockType = keys.UpdateLock
    fdblock.Sid = sid

    // log.Println("pre lock", fdblock)

    retryNum := 10
    for i := 0; i < retryNum; i++ {
      _, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
        err := L.Lock(tr, &fdblock)
        if err != nil {
          return nil, err
        }
        return nil, nil
      })

      if err == ErrorRetry {
        log.Printf("pre lock and retry %d", i)
        continue
      } else {
        break
      }
    }
  }
  return err
}

func (L *LockMgr) DoWithAutoLock(db fdb.Database, lock *keys.Lock, f func(fdb.Transaction) (interface{}, error), retryNum int) (data interface{}, err error) {
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

func (L *LockMgr) Lock(tr fdb.Transaction, lock *keys.Lock) error {
	return L.TryLockAndWatch(tr, lock)
}

func (L *LockMgr) CheckConflicts(tr fdb.Transaction, checkLock *keys.Lock, realType uint8) (lockBefore bool, err error) {
	prefix, err := fdbkv.MarshalRangePerfix(checkLock)
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
		fdblock := &keys.Lock{}
		err := fdbkv.UnmarshalKey(data.Key, fdblock)
		if err != nil {
			log.Printf("Unable to UnmarshalKey: %v %v\n", data, err)
			return false, err
		}

		log.Printf("get other lock: %v\n", fdblock)

		// 支持重入
		if fdblock.Sid == checkLock.Sid && fdblock.LockType == realType {
			log.Printf("reentry %v\n", checkLock)
			return true, nil
		}

		if keys.LockConflicts(fdblock.LockType, realType) {
	    kvOp := NewKvOperator(tr)
      var plock keys.PotentialLock
      plock.Database = checkLock.Database
      plock.Relation = checkLock.Relation
      plock.LockType = checkLock.LockType
      plock.Sid = checkLock.Sid
      err := kvOp.Write(&plock, &fdbkv.EmptyValue{})
			if err != nil {
				return false, err
			}

			fut := tr.Watch(data.Key)
			log.Printf("LockConflicts Watch: %s\n", data.Key)
			tr.Commit()
			fut.BlockUntilReady()

			err = fut.Get()
			if err != nil {
				return false, err
			}
			log.Printf("Retry, LockConflicts key: %s\n", data.Key)
			return false, ErrorRetry
		}
	}
	return false, nil
}

func (L *LockMgr) TryLockAndWatch(tr fdb.Transaction, lock *keys.Lock) error {
	sessOp := NewSessionOperator(tr, lock.Sid)
	sess, err := sessOp.CheckAndGet(keys.SessionTransactionStart)
	if err != nil {
		log.Printf("CheckAndGet error %v %v %v %v", err, L, sessOp, lock)
		return err
	}
	log.Printf("lock in session %v", sess)

	var checkLock keys.Lock
	checkLock.Database = lock.Database
	checkLock.Relation = lock.Relation
	checkLock.Sid = lock.Sid
	conflictsTypes := keys.GetConflictsLocks(lock.LockType)

	for _, conflictsType := range conflictsTypes {
		checkLock.LockType = conflictsType
		// log.Println(checkLock, conflictsTypes)
		lockBefore, err := L.CheckConflicts(tr, &checkLock, lock.LockType)
		if err != nil {
			return err
		}
		if lockBefore {
			log.Printf("lockBefore, do not need write")
			return nil
		}
	}

	kvOp := NewKvOperator(tr)
	return kvOp.Write(lock, &fdbkv.EmptyValue{})
}

func (M *LockMgr) Unlock(tr fdb.Transaction, lock *keys.Lock) error {
	kvOp := NewKvOperator(tr)
	return kvOp.Delete(lock)
}

func (M *LockMgr) UnlockAll(tr fdb.Transaction, database uint64, sid uint64) error {
  log.Printf("unlock all database:%d sid:%d", database, sid)

	var lock keys.Lock
	lock.Database = database
	kvOp := NewKvOperator(tr)
	prefix, err := fdbkv.MarshalRangePerfix(&lock)
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

	log.Println("get database locks", kvs)
	// Advance will return true until the iterator is exhausted
	for _, data := range kvs {
		fdblock := &keys.Lock{}
		err := fdbkv.UnmarshalKey(data.Key, fdblock)
		if err != nil {
			return err
		}
		/*
			err = fdbkv.UnmarshalValue(data.Value, fdblock)
			if err != nil {
				return err
			}
		*/
		if fdblock.Sid == sid {
			log.Println("delete lock ", fdblock)
			kvOp.Delete(fdblock)
		}
	}
	return nil
}
