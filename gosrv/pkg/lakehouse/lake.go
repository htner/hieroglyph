package lakehouse

import (
	"github.com/htner/sdb/gosrv/pkg/transaction"
	"github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type LakeRelOperator struct {
	t transaction.Transaction
	s kvpair.Session
}

func (L *LakeRelOperator) NewFiles(sid type.SessionId, num int) error {
	// 上锁
	database := files[0].Database
	transcation := NewTranscation(databaseid, sid)

	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		Lock(tr, database, Lock.Read)

		InsertFiles()
	}
	return nil
}

func (L *LakeRelOperator) InsertFiles(sid type.SessionId, files []*FileMeta) error {
	// 上锁
	database := files[0].Database
	transcation := NewTranscation(databaseid, sid)

	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		Lock(tr, database, Lock.Read)

		InsertFiles()
	}
	return nil
}

func (L *LakeRelOperator) DeleleFile(f *File) error {
	return nil
}

func (L *LakeRelOperator) ReplaceFile(newFile *File, oldFile *File) error {
	return nil
}

func (L *LakeRelOperator) GetAllFileForRead(xid types.TransactionId) {
}

func (L *LakeRelOperator) GetAllFileForWrite(xid types.TransactionId) {
}

func (L *LakeRelOperator) GetDeltaFile(oldXid types.TransactionId, newXid types.TransactionId) {
}


func (t *Transaction) WriteAndDelete(insertKVs []*kv.MvccKVPair, deleteKeys []*kv.MvccKey) error {
	for _, kvpair := range insertKVs {
		kvpair.K.Xmin = t.Xid
		kvpair.V.XminStatus = XS_START
		kvpair.V.Xmax = InvaildTranscaton
		kvpair.V.XmaxStatus = XS_NULL
	}

	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		e, tkv := t.CheckVaild(tr)
		if e != nil {
			return nil, e
		}
		// insert 可以直接写，因为前面已经检查Xmin的合法性
		for _, kv := range insertKVs {
			tran := NewMvccOperator(tr, kv.K, kv.V)
			e = tran.Insert()
			if e != nil {
				return nil, e
			}
			e = t.addWALForUpdate(tr, tkv, kv.K)
			if e != nil {
				return nil, e
			}
		}

		for _, deleteKey := range deleteKeys {
			var value kv.MvccValue
			tran := NewMvccOperator(tr, deleteKey, &value)
			e = tran.DeleteMark(t.Xid)
			if e != nil {
				return nil, e
			}
			e = t.addWALForUpdate(tr, tkv, deleteKey)
			if e != nil {
				return nil, e
			}
		}
		// 更新UpdateKeySeq
		baseTran := NewKvOperator(tr, tkv, tkv)
		err := baseTran.Write()
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return e
}

// 更为通用的接口
func (t *Transaction) WriteAndAutoDeletes(kvs []*kv.MvccKVPair) error {
	for _, kvpair := range kvs {
		kvpair.K.Xmin = t.Xid
		kvpair.V.XminStatus = XS_START
		kvpair.V.Xmax = InvaildTranscaton
		kvpair.V.XmaxStatus = XS_NULL
	}

	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		sr := NewSnapshotRead(tr)
		deleteKVs := make([]*kv.MvccKVPair, 0)
		for _, kv := range kvs {
			keyVals, err := sr.SnapshotReadAll(*kv.K)
			if err != nil {
				return nil, err
			}
			for _, keyval := range keyVals {
				if keyval.V.Xmax != 0 {
					continue
				}
				keyval.V.Xmax = t.Xid
				keyval.V.XmaxStatus = XS_START

				deleteKVs = append(deleteKVs, keyval)
			}
		}
		kvs = append(kvs, deleteKVs...)
		err := t.innerWrite(kvs, nil, tr)
		return nil, err
	})
	return e
}

// 处理单个kv的情况
func (t *Transaction) WriteAndAutoDelete(kvpair *kv.MvccKVPair) error {
	kvpair.K.Xmin = t.Xid
	kvpair.V.XminStatus = XS_START
	kvpair.V.Xmax = InvaildTranscaton
	kvpair.V.XmaxStatus = XS_NULL

	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {

		sr := NewSnapshotRead(tr)
		kvs := make([]*kv.MvccKVPair, 0)
		deleteKVs := make([]*kv.MvccKVPair, 0)
		keyVals, err := sr.SnapshotReadAll(*kvpair.K)
		if err != nil {
			return nil, err
		}
		for _, keyval := range keyVals {
			if keyval.V.Xmax != 0 {
				continue
			}
			keyval.V.Xmax = t.Xid
			keyval.V.XmaxStatus = XS_START

			deleteKVs = append(deleteKVs, keyval)
		}
		kvs = append(kvs, kvpair)
		err = t.innerWrite(kvs, deleteKVs, tr)
		return nil, err
	})
	return e
}

// 这种情况是内部系统认为不会有冲突
func (t *Transaction) Write(kvpair *kv.MvccKVPair) error {
	kvpair.K.Xmin = t.Xid
	kvpair.V.XminStatus = XS_START
	kvpair.V.Xmax = InvaildTranscaton
	kvpair.V.XmaxStatus = XS_NULL

	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvs := make([]*kv.MvccKVPair, 0)
		kvs = append(kvs, kvpair)
		return nil, t.innerWrite(kvs, nil, tr)
	})
	return e
}

func (t *Transaction) innerWrite(insertKVs []*kv.MvccKVPair, deleteKVs []*kv.MvccKVPair, tr fdb.Transaction) error {
	// FIXME_MVP 判断所有的key是不是合法的
	// 检查 insertKVs key 的 Xmin
	// 检查 deleteKVs Xmax 的 Xmax == 0, XminStatus == COMMIT,
	e, tkv := t.CheckVaild(tr)
	if e != nil {
		return e
	}
	// insert 可以直接写，因为前面已经检查Xmin的合法性
	for _, kv := range insertKVs {
		tran := NewMvccOperator(tr, kv.K, kv.V)
		e = tran.Insert()
		if e != nil {
			return e
		}
		e = t.addWALForUpdate(tr, tkv, kv.K)
		if e != nil {
			return e
		}
	}

	for _, delKVPair := range deleteKVs {
		tran := NewMvccOperator(tr, delKVPair.K, delKVPair.V)
		e = tran.Insert()
		if e != nil {
			return e
		}
		/*
			var value kv.MvccValue
			tran := NewMvccTran(tr, delK, &value)
			e = tran.DeleteMark(t.xid)
			if e != nil {
				return e
			}
		*/
		e = t.addWALForUpdate(tr, tkv, delKVPair.K)
		if e != nil {
			return e
		}
	}

	baseTran := NewKvOperator(tr, tkv, tkv)
	err := baseTran.Write()
	if err != nil {
		return err
	}
	return nil
}