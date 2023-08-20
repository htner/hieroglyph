package lakehouse

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
)

func TestLake_1(t *testing.T) { // 模拟需要耗时一秒钟运行的任务
	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
	sess := kv.NewSession(1)
	sess.Id = 1
	sess.Uid = 1
	sess.Token = []byte("")
	sess.State = kv.SessionTransactionIdle

	db, err := fdb.OpenDatabase(macfile)
	if err != nil {
		t.Logf("lake1 open error %s", err.Error())
		return
	}
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		sessOp := NewSessionOperator(tr, 1)
		sessOp.Write(sess)
		return nil, nil
	})

	tr := NewTranscation(1, 1)
	err = tr.Start(true)
	if err != nil {
		t.Fatalf("new transaction error:%v", err)
	}

	lakeop := NewLakeRelOperator(1, 1, 1)

	var files []string
	files = append(files, "1.p")
	files = append(files, "1.p")
	t.Logf("start mark files")
	err = lakeop.MarkFiles(1, files)
	if err != nil {
		t.Fatalf("mark files %v", err)
	}

	var fileMeta kv.FileMeta
	fileMeta.Database = 1
	fileMeta.Relation = 1
	fileMeta.Filename = "1.p"

	var fileMetas []*kv.FileMeta
	fileMetas = make([]*kv.FileMeta, 0)

	fileMetas = append(fileMetas, &fileMeta)
	fileMeta2 := fileMeta
	fileMeta.Filename = "2.p"
	fileMetas = append(fileMetas, &fileMeta2)

	t.Log("start insert files")
	lakeop.InsertFiles(1, fileMetas)

	fileMeta.Filename = ""

	t.Logf("start getall files")
	fileMetas, _, err = lakeop.GetAllFileForRead(1, &fileMeta)
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Logf("%v", fileMetas)
}
