package transaction

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
)

type WalOperator struct {
	t fdb.Transaction
	w *kv.WAL
}

func NewWalOperator(tr fdb.Transaction, w *kv.WAL) *WalOperator {
	return &WalOperator{t: tr, w: w}
}

func (wt *WalOperator) GetBatchWAL(tr fdb.Transaction, batch int) ([]*kv.WAL, error) {
	key, err := kv.MarshalRangePerfix(wt.w)
	if err != nil {
		return nil, err
	}
	pr, err := fdb.PrefixRange(key)
	if err != nil {
		return nil, err
	}
	kvs, err := tr.GetRange(pr, fdb.RangeOptions{Limit: batch}).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	wals := make([]*kv.WAL, 0)
	for _, kvPair := range kvs {
		var wal kv.WAL
		err = kv.UnmarshalKey(kvPair.Key, &wal)
		if err != nil {
			return nil, err
		}
		err = kv.UnmarshalValue(kvPair.Value, &wal)
		if err != nil {
			return nil, err
		}
		wals = append(wals, &wal)
	}
	return wals, nil
}
