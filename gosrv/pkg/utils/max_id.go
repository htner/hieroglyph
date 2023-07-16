package utils

import (
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
)

type MaxIdOperator struct {
	tr fdb.Transaction
  key kvpair.FdbKey
}

func NewMaxIdOperator(tr fdb.Transaction, key kvpair.FdbKey) (L *MaxIdOperator) {
  return &MaxIdOperator{tr:tr, key: key}
}

func (I* MaxIdOperator) GetCurrent() (uint64, error) {
  kvOp := fdbkv.NewKvOperator(I.tr)
  var max_id kvpair.MaxId
  err := kvOp.Read(I.key, &max_id)
  if err != nil {
    if err != fdbkv.EmptyDataErr {
      log.Println("read maxfile id error:", err)
      return 0, err
    }
  }
  return max_id.Value, nil
}

func (I* MaxIdOperator) GetNext() (uint64, error) {
  kvOp := fdbkv.NewKvOperator(I.tr)
  var max_id kvpair.MaxId
  err := kvOp.Read(I.key, &max_id)
  if err != nil {
    if err != fdbkv.EmptyDataErr {
      log.Println("read maxfile id error:", err)
      return 0, err
    }
  }
  max_id.Value += 1
  err = kvOp.Write(I.key, &max_id)
  return max_id.Value, err 
}

