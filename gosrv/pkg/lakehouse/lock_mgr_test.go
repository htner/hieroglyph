package lakehouse

import (
  "testing"
  "time"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func TestLock(t *testing.T) {
  var mgr LockMgr

	db, err := fdb.OpenDefault()
	if err != nil {
    t.Logf("TestLock open error", err.Error())
    return
	}

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1 
	fdblock.LockType = InsertLock
	
	data, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    err := mgr.Lock(tr, &fdblock)
    if err != nil {
      t.Logf("TestLock lock error", err.Error())
    }
    time.Sleep(1 * time.Second)

    err = mgr.Unlock(tr, &fdblock)
    if err != nil {
      t.Logf("TestLock unlock error", err.Error())
    }
    return nil, nil
  })
}

func TestParallel_0(t *testing.T) {// 模拟需要耗时一秒钟运行的任务  
  t.Parallel()// 调用Parallel函数，以并行方式运行测试用例  
  var mgr LockMgr

	db, err := fdb.OpenDefault()
	if err != nil {
    t.Logf("TestParallel_0 open error", err.Error())
    return
	}

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1 
	fdblock.LockType = InsertLock
	
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    err := mgr.Lock(tr, &fdblock)
    if err != nil {
      t.Logf("TestParallel_0 lock error", err.Error())
    }
    time.Sleep(1 * time.Second)

    err = mgr.Unlock(tr, &fdblock)
    if err != nil {
      t.Logf("TestParallel_0 unlock error", err.Error())
    }
    return nil, nil
  })
}

func TestParallel_1(t *testing.T) {// 模拟需要耗时一秒钟运行的任务  
  t.Parallel()// 调用Parallel函数，以并行方式运行测试用例  
  time.Sleep(1 * time.Second)

  var mgr LockMgr
  db, err := fdb.OpenDefault()
	if err != nil {
    t.Logf("TestParallel_0 open error", err.Error())
    return
	}

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1 
	fdblock.LockType = UpdateLock
	
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    err := mgr.Lock(tr, &fdblock)
    if err != nil {
      t.Logf("TestParallel_0 lock error", err.Error())
    }
    time.Sleep(1 * time.Second)

    err = mgr.Unlock(tr, &fdblock)
    if err != nil {
      t.Logf("TestParallel_0 unlock error", err.Error())
    }
    return nil, nil
  })

}

func TestParallel_2(t *testing.T) { // 模拟需要耗时2秒运行的任务 
  t.Parallel()  
  time.Sleep(2 * time.Second)
  var mgr LockMgr
  db, err := fdb.OpenDefault()
	if err != nil {
    t.Logf("TestParallel_0 open error", err.Error())
    return
	}

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1 
	fdblock.LockType = UpdateLock
	
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    err := mgr.Lock(tr, &fdblock)
    if err != nil {
      t.Logf("TestParallel_0 lock error", err.Error())
    }
    time.Sleep(1 * time.Second)

    err = mgr.Unlock(tr, &fdblock)
    if err != nil {
      t.Logf("TestParallel_0 unlock error", err.Error())
    }
    return nil, nil
  })

}

func TestParallel_3(t *testing.T) {// 模拟需要耗时3秒运行的任务 
    t.Parallel()  
  var mgr LockMgr
  db, err := fdb.OpenDefault()
	if err != nil {
    t.Logf("TestParallel_0 open error", err.Error())
    return
	}

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1 
	fdblock.LockType = ReadLock
	
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    err := mgr.Lock(tr, &fdblock)
    if err != nil {
      t.Logf("TestParallel_0 lock error", err.Error())
    }
    time.Sleep(1 * time.Second)

    err = mgr.Unlock(tr, &fdblock)
    if err != nil {
      t.Logf("TestParallel_0 unlock error", err.Error())
    }
    return nil, nil
  })

}
