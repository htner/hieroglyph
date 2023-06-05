package lakehouse

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/lakehouse/kvpair"
)

type KvOperator struct {
	t fdb.Transaction
}

func NewKvOperator(tr fdb.Transaction) *KvOperator {
	return &KvOperator{t: tr}
}

func (t *KvOperator) Write(key kv.FdbKey, value kv.FdbValue) error {
	sKey, err := kv.MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	sValue, err := kv.MarshalValue(value)
	if err != nil {
		return err
	}
	t.t.Set(fKey, sValue)
	return nil
}

func (t *KvOperator) Delete(key kv.FdbKey) error {
	sKey, err := kv.MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	t.t.Clear(fKey)
	return nil
}

var KvNotFound = errors.New("kv not found")

func (t *KvOperator) Read(key kv.FdbKey, value kv.FdbValue) error {
	sKey, err := kv.MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	future := t.t.Get(fKey)

	v, e := future.Get()
	if e != nil {
		return KvNotFound
	}
	return kv.UnmarshalValue(v, value)
}
