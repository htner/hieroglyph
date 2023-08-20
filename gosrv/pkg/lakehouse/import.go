package lakehouse

import (
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func NewKvOperator(tr fdb.Transaction) *fdbkv.KvOperator {
	return fdbkv.NewKvOperator(tr)
}
