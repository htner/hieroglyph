package utils

import (
	"errors"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
)

type MaxIdOperator struct {
	tr        fdb.Transaction
	key       fdbkv.FdbKey
	current   uint64
	init      bool
	localnext bool
}

func NewMaxIdOperator(tr fdb.Transaction, key fdbkv.FdbKey) (L *MaxIdOperator) {
	return &MaxIdOperator{tr: tr, key: key, current: 0, init: false, localnext: false}
}

func (I *MaxIdOperator) GetCurrent() (uint64, error) {
	kvOp := fdbkv.NewKvOperator(I.tr)
	var max_id keys.MaxId
	err := kvOp.Read(I.key, &max_id)
	if err != nil {
		if err != fdbkv.ErrEmptyData {
			log.Println("read maxfile id error:", err)
			return 0, err
		}
		//max_id.Value = 10240
	}
	I.current = max_id.Value
	I.init = true
	return max_id.Value, nil
}

func (I *MaxIdOperator) GetNext() (uint64, error) {
	kvOp := fdbkv.NewKvOperator(I.tr)
	var max_id keys.MaxId
	err := kvOp.Read(I.key, &max_id)
	if err != nil {
		if err != fdbkv.ErrEmptyData {
			log.Println("read maxfile id error:", err)
			return 0, err
		}
		//max_id.Value = 10240
	}
	max_id.Value += 1
	err = kvOp.Write(I.key, &max_id)
	I.current = max_id.Value
	I.init = true
	return max_id.Value, err
}

func (I *MaxIdOperator) GetLocalNext() (uint64, error) {
	if !I.init {
		return 0, errors.New("not init")
	}
	I.current++
	I.localnext = true
	return I.current, nil
}

// TODO 并发问题还没有解决
func (I *MaxIdOperator) Sync() error {
	if !I.init {
		return errors.New("not init")
	}
	if !I.localnext {
		return nil
	}
	kvOp := fdbkv.NewKvOperator(I.tr)
	var max_id keys.MaxId
	max_id.Value = I.current
	return kvOp.Write(I.key, &max_id)
}

func (I *MaxIdOperator) Add(count uint64) (uint64, uint64, error) {
	kvOp := fdbkv.NewKvOperator(I.tr)
	var max_id keys.MaxId
	err := kvOp.Read(I.key, &max_id)
	if err != nil {
		if err != fdbkv.ErrEmptyData {
			log.Println("read maxfile id error:", err)
			return 0, 0, err
		}
		//I.current = 10240
	}
	start := max_id.Value + 1
	max_id.Value += count
	err = kvOp.Write(I.key, &max_id)
	I.current = max_id.Value
	I.init = true
	return start, max_id.Value, err
}
