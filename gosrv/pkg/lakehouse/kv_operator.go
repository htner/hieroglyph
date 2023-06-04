package transaction

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
)

type KvOperator struct {
	tran fdb.Transaction

}

func NewKvOperator(tr fdb.Transaction) *KvOperator {
	return &KvOperator{tran: tr}
}

func (t *KvOperator) Write(k kv.FdbKey, v kv.FdbValue) error {
	sKey, err := kv.MarshalKey(k)

	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	sValue, err := kv.MarshalValue(v)
	if err != nil {
		return err
	}
	t.tran.Set(fKey, sValue)
	return nil
}

func (t *KvOperator) Delete(k kv.FdbKey) error {
	sKey, err := kv.MarshalKey(k)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	t.tran.Clear(fKey)
	return nil
}

var KvNotFound = errors.New("kv not found")

func (t *KvOperator) Read(k kv.FdbKey, v kv.FdbValue) error {
	sKey, err := kv.MarshalKey(k)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	future := t.tran.Get(fKey)

	value, e := future.Get()
	if e != nil {
		return KvNotFound
	}
	return kv.UnmarshalValue(value, v)
}
