package lakehouse

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
)

func NewKvOperator(tr fdb.Transaction) *fdbkv.KvOperator {
	return fdbkv.NewKvOperator(tr)
}
