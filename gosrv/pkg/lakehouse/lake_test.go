package lakehouse

import (
	"testing"

	kv "github.com/htner/sdb/gosrv/pkg/lakehouse/kvpair"
)

func TestLake_1(t *testing.T) { // 模拟需要耗时一秒钟运行的任务

	t.Parallel() // 调用Parallel函数，以并行方式运行测试用例
	lakeop := NewLakeRelOperator(1, 1)

	var files []string
	files = append(files, "1.p")
	files = append(files, "1.p")
	t.Log("start makr files")
	err := lakeop.MarkFiles(1, files)
	if err != nil {
		t.Errorf("%v", err)
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

	t.Log("start getall files")
	fileMetas, _, err = lakeop.GetAllFileForRead(1, &fileMeta)
	if err != nil {
		t.Errorf("%v", err)
	}
	t.Logf("%v", fileMetas)
}
