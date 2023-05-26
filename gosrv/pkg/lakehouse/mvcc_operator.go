package transaction

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type MvccOperator struct {
	t fdb.Transaction
	k *kv.MvccKey
	v *kv.MvccValue
}

func NewMvccOperator(tr fdb.Transaction, key *kv.MvccKey, val *kv.MvccValue) *MvccOperator {
	return &MvccOperator{t: tr, k: key, v: val}
}

func (tr *MvccOperator) Insert() error {
	bKey, err := kv.MarshalKey(tr.k)
	fKey := fdb.Key(bKey)
	if err != nil {
		return err
	}

	bValue, err := kv.MarshalValue(tr.v)
	if err != nil {
		return err
	}
	tr.t.Set(fKey, bValue)
	return nil
}

func (tr *MvccOperator) DeleteMark(tid types.TransactionId) error {

	bKey, err := kv.MarshalKey(tr.k)
	fKey := fdb.Key(bKey)
	if err != nil {
		return err
	}

	if tr.k.Xmin == tid {
		tr.t.Clear(fKey)
		return nil
	}

	future := tr.t.Get(fKey)
	value, err := future.Get()
	if err == nil {
		return errors.New("session id confilct")
	}

	tr.v = &kv.MvccValue{}
	err = kv.UnmarshalValue(value, tr.v)
	if err != nil {
		return err
	}

	tr.v.Xmax = tid
	tr.v.XmaxStatus = XS_START

	sValue, err := kv.MarshalValue(tr.v)
	if err != nil {
		return err
	}
	tr.t.Set(fKey, sValue)
	return nil
}
