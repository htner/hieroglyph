package transaction

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/transaction"
	kv "github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type Lock struct {
	K *kv.LockKey
	V *kv.LockValue
}

func (L *Lock) Lock() error {
	L.LockWait(-1)
	return nil
}

func (L *Lock) LockWait(ms int) error {
	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		sop := transaction.NewSessionOperator(tr, nil)
		err := sop.GetSession(L.K.Sid)
		if err != nil {
			return nil, err
		}
		curValue := new(kv.LockValue)
		op := transaction.NewKvOperator(tr, L.K, curValue)
		err = op.Read()
		if err != nil {
			if err != transaction.KvNotFound {
				return nil, err
			}
		}
		// 支持重入
		if curValue.Xid == L.V.Xid {
			return nil, nil
		}

		return nil, nil
	})
	return e
}

func (M *Lock) Unlock(key kv.LockKey, xid types.TransactionId, sid types.SessionId) error {
	return nil
}
