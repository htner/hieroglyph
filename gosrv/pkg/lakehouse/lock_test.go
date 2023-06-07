package lakehouse

import (
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/types"
)

const macfile string = ""

var xid types.TransactionId = 1 

func StartLock(t *testing.T, lockType uint8, name string) {
	var mgr LockMgr
	db, err := fdb.OpenDatabase(macfile)
	if err != nil {
		t.Logf("%s open error %s", name, err.Error())
		return
	}

  xid++

	var fdblock Lock
	fdblock.Database = 1
	fdblock.Relation = 1
  fdblock.Sid = 1
  fdblock.Xid = xid 
	fdblock.LockType = lockType 

	var retry int = 0 

	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    t.Logf("%s retry %d", name, retry)
    retry++
		err := mgr.Lock(tr, &fdblock)
		if err != nil {
			t.Logf("%s lock error %s", name, err.Error())
			return nil, err
		}
		t.Logf("%s Lock ok", name)
		return nil, nil
	})

	// do something
	time.Sleep(1 * time.Second)

	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err = mgr.Unlock(tr, &fdblock)
		//t.Logf("%s UnLock ok", name)
		if err != nil {
			t.Logf("%s unlock error %s", name, err.Error())
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		t.Logf("%s Transact error %s", name, err.Error())
	}
}

func TestParallel_0(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, InsertLock, "test_insert_0")
}

func TestParallel_1(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, InsertLock, "test_insert_1")
}

func TestParallel_2(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, InsertLock, "test_insert_2")
}

func TestParallel_3(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, InsertLock, "test_insert_3")
}

func TestParallel_4(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, UpdateLock, "test_update_0")
}

func TestParallel_5(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, UpdateLock, "test_update_1")
}

func TestParallel_6(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, UpdateLock, "test_update_2")
}

func TestParallel_7(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, UpdateLock, "test_update_3")
}

func TestParallel_8(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, UpdateLock, "test_update_4")
}

func TestParallel_9(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, ReadLock, "test_read_0")
}

func TestParallel_10(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, ReadLock, "test_read_1")
}

func TestParallel_11(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, ReadLock, "test_read_2")
}

func TestParallel_12(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, ReadLock, "test_read_3")
}

func TestParallel_13(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
  StartLock(t, ReadLock, "test_read_4")
}


func TestMain(m *testing.M) {
	fdb.MustAPIVersion(710)
	m.Run()
}
