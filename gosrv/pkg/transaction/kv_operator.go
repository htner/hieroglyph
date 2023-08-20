package transaction

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
)

type KvOperator struct {
	tran fdb.Transaction
	k    kv.FdbKey
	v    kv.FdbValue
}

func NewKvOperator(tr fdb.Transaction, k kv.FdbKey, v kv.FdbValue) *KvOperator {
	return &KvOperator{tran: tr, k: k, v: v}
}

func (t *KvOperator) Write() error {
	sKey, err := kv.MarshalKey(t.k)

	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	sValue, err := kv.MarshalValue(t.v)
	if err != nil {
		return err
	}
	t.tran.Set(fKey, sValue)
	return nil
}

func (t *KvOperator) Delete() error {
	sKey, err := kv.MarshalKey(t.k)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	t.tran.Clear(fKey)
	return nil
}

var KvNotFound = errors.New("kv not found")

func (t *KvOperator) Read() error {
	sKey, err := kv.MarshalKey(t.k)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	future := t.tran.Get(fKey)

	value, e := future.Get()
	if e != nil {
		return KvNotFound
	}
	return kv.UnmarshalValue(value, t.v)
}
