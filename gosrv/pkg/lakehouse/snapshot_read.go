package transaction

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
)

type SnapshotRead struct {
	tr fdb.Transaction
}

func NewSnapshotRead(tr fdb.Transaction) *SnapshotRead {
	return &SnapshotRead{tr: tr}
}

func (sr *SnapshotRead) SnapshotReadAll(k kv.MvccKey) ([]*kv.MvccKVPair, error) {
	key, err := kv.MarshalRangePerfix(&k)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	pr, err := fdb.PrefixRange(key)
	if err != nil {
		return nil, err
	}
	kvs, err := sr.tr.GetRange(pr, fdb.RangeOptions{}).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	vals := make([]*kv.MvccKVPair, 0)
	for _, kvPair := range kvs {
		var val kv.MvccKVPair
		val.K = new(kv.MvccKey)
		val.V = new(kv.MvccValue)

		err = kv.UnmarshalKey(kvPair.Key, val.K)
		if err != nil {
			return nil, err
		}

		err = kv.UnmarshalValue(kvPair.Value, val.V)
		if err != nil {
			return nil, err
		}
		vals = append(vals, &val)
	}
	return vals, nil
}
