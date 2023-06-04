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


type WALMvccDetail struct {
	wal   *kv.WAL
	vbyte []byte
	k     *kv.MvccKey
	v     *kv.MvccValue
}

type WALMvccDetails struct {
	details []*WALMvccDetail
}

func (t *Transaction) getMvccDetails() (*WALMvccDetails, error) {
	db := fdb.MustOpenDefault()
	obj, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		var wal kv.WAL
		wal.Xid = t.Xid
		walTran := NewWalOperator(tr, &wal)

		wals, err := walTran.GetBatchWAL(tr, 100)
		if err != nil {
			return nil, err
		}

		future := tr.Commit()
		err = future.Get()
		if err != nil {
			return nil, err
		}
		tr.Reset()

		futures := make([]fdb.FutureByteSlice, 0)
		for _, wal := range wals {
			future := tr.Get(fdb.Key(wal.Log))
			futures = append(futures, future)
		}

		D := &WALMvccDetails{}
		D.details = make([]*WALMvccDetail, 0)

		for index, wal := range wals {
			future := futures[index]
			val, err := future.Get()
			if err != nil {
				return nil, err
			}
			var detail WALMvccDetail
			detail.wal = wal
			detail.vbyte = val

			D.details = append(D.details, &detail)
		}

		return D, nil
	})
	if err != nil {
		return nil, err
	}
	res, ok := obj.(WALMvccDetails)
	if !ok {
		return nil, errors.New("")
	}
	return &res, nil
}

func (t *Transaction) PrepareMvccDetails(D *WALMvccDetails, status uint8) error {
	for _, elem := range D.details {
		var key kv.MvccKey
		var value kv.MvccValue
		err := kv.UnmarshalKey(elem.wal.Log, &key)
		if err != nil {
			return err
		}
		err = kv.UnmarshalValue(elem.vbyte, &value)
		if err != nil {
			return err
		}

		if key.Xmin == t.Xid {
			if status == XS_COMMIT {
				value.XminStatus = XS_COMMIT
			} else { /* XS_ABORT */
				// TODO 删除文件
				// call external function
				value.XminStatus = XS_ABORT
			}
		} else if value.Xmax == t.Xid {
			if status == XS_COMMIT {
				value.XmaxStatus = XS_COMMIT
			} else { /* XS_ABORT */
				value.XmaxStatus = XS_ABORT
				// for MVCC, do external nothing here
			}
		} else {
			// 严重的错误？
		}
	}
	return nil
}

func (t *Transaction) FlushMvcc(D *WALMvccDetails) error {
	db := fdb.MustOpenDefault()
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for _, elem := range D.details {
			tran := NewKvOperator(tr, elem.k, elem.v)
			err := tran.Write()
			if err != nil {
				return nil, err
			}
			tran.k = elem.wal
			tran.v = elem.wal
			err = tran.Delete()
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

func (t *Transaction) DeleteTransactionLast() error {
	db := fdb.MustOpenDefault()
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		var tkv kv.TransactionInfo
		tkv.Tid = t.Xid
		baseTran := NewKvOperator(tr, &tkv, &tkv)
		err := baseTran.Read()
		if err != nil {
			return err, nil
		}
		if tkv.Status != XS_START {
			return errors.New(""), nil
		}
		return nil, nil
	})
	return err
}

func (t *Transaction) CommitKV(status uint8) error {
	if status != XS_COMMIT && status != XS_ABORT {
		return errors.New("")
	}
	for true {
		detail, err := t.getMvccDetails()
		if err != nil {
			return err
		}
		if len(detail.details) == 0 {
			break
		}
		err = t.PrepareMvccDetails(detail, status)
		if err != nil {
			return err
		}
		err = t.FlushMvcc(detail)
		if err != nil {
			return err
		}
	}
	t.DeleteTransactionLast()
	return nil
}

func (t *Transaction) addWALForUpdate(tr fdb.Transaction, tkv *kv.TransactionInfo, mvccKey *kv.MvccKey) error {
	sKey, err := kv.MarshalKey(mvccKey)
	if err != nil {
		return err
	}

	tkv.UpdateKeySeq = tkv.UpdateKeySeq + 1
	wal := &kv.WAL{Xid: tkv.Tid, Seqid: tkv.UpdateKeySeq, Log: sKey}
	baseTran := NewKvOperator(tr, wal, wal)
	return baseTran.Write()
}
