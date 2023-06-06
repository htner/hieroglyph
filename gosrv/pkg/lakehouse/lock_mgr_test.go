package lakehouse

import (
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const macfile string = "/usr/local/etc/foundationdb/fdb.cluster"

func TestLock(t *testing.T) {
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
	var mgr LockMgr
	db, err := fdb.OpenDatabase(macfile)
	if err != nil {
		t.Logf("TestLock open error %s", err.Error())
		return
	}

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1
	fdblock.LockType = InsertLock

	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err := mgr.Lock(tr, &fdblock)
		if err != nil {
			t.Logf("TestLock lock error %s", err.Error())
			return nil, err
		}
		t.Logf("TestLock InsertLock Lock ok")
		time.Sleep(1 * time.Second)

		err = mgr.Unlock(tr, &fdblock)
		if err != nil {
			t.Logf("TestLock unlock error %s", err.Error())
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		t.Logf("TestLock Transact error %s", err.Error())
	}
}

func TestParallel_0(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
	var mgr LockMgr

	db, err := fdb.OpenDatabase(macfile)
	if err != nil {
		t.Logf("TestParallel_0 open error %s", err.Error())
		return
	}

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1
	fdblock.LockType = InsertLock

	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err := mgr.Lock(tr, &fdblock)
		if err != nil {
			t.Logf("TestParallel_0 lock error %s", err.Error())
			return nil, err
		}
		t.Logf("TestParallel_0 InsertLock Lock ok")
		time.Sleep(1 * time.Second)

		err = mgr.Unlock(tr, &fdblock)
		if err != nil {
			t.Logf("TestParallel_0 unlock error  %s", err.Error())
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		t.Logf("TestParallel_0 Transact error %s", err.Error())
	}
}

func TestParallel_1(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例

	var mgr LockMgr
	db, err := fdb.OpenDatabase(macfile)
	if err != nil {
		t.Logf("TestParallel_1 open error %s", err.Error())
		return
	}

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1
	fdblock.LockType = UpdateLock

	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err := mgr.Lock(tr, &fdblock)
		if err != nil {
			t.Logf("TestParallel_1 lock error %s", err.Error())
			return nil, err
		}
		t.Logf("TestParallel_1 UpdateLock Lock ok")
		time.Sleep(1 * time.Second)

		err = mgr.Unlock(tr, &fdblock)
		if err != nil {
			t.Logf("TestParallel_1 unlock error %s", err.Error())
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		t.Logf("TestParallel_1 Transact error %s", err.Error())
	}
}

func TestParallel_2(t *testing.T) { // 模拟需要耗时2秒运行的任务
	t.Parallel()
	var mgr LockMgr
	db, err := fdb.OpenDatabase(macfile)
	if err != nil {
		t.Logf("TestParallel_2 open error %s", err.Error())
		return
	}

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1
	fdblock.LockType = UpdateLock

	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err := mgr.Lock(tr, &fdblock)
		if err != nil {
			t.Logf("TestParallel_2 UpdateLock lock error %s", err.Error())
			return nil, err
		}
		t.Logf("TestParallel_2 Lock ok")
		time.Sleep(1 * time.Second)

		err = mgr.Unlock(tr, &fdblock)
		if err != nil {
			t.Logf("TestParallel_2 unlock error %s", err.Error())
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		t.Logf("TestParallel_2 Transact error %s", err.Error())
	}
}

func TestParallel_3(t *testing.T) { // 模拟需要耗时3秒运行的任务
	t.Parallel()
	var mgr LockMgr
	db, err := fdb.OpenDatabase(macfile)
	if err != nil {
		t.Logf("TestParallel_3 open error %s", err.Error())
		return
	}

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1
	fdblock.LockType = ReadLock

	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err := mgr.Lock(tr, &fdblock)
		if err != nil {
			t.Logf("TestParallel_3 lock error %s", err.Error())
			return nil, err
		}
		t.Logf("TestParallel_3 ReadLock Lock ok")
		time.Sleep(1 * time.Second)

		err = mgr.Unlock(tr, &fdblock)
		if err != nil {
			t.Logf("TestParallel_3 unlock error %s", err.Error())
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		t.Logf("TestParallel_3 Transact error %s", err.Error())
	}

}

func TestMain(m *testing.M) {
	fdb.MustAPIVersion(630)
	m.Run()
}
