package transaction

import (
	"errors"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

const (
	XS_NULL   uint8 = 0
	XS_INIT   uint8 = 1
	XS_START  uint8 = 2
	XS_COMMIT uint8 = 3
	XS_ABORT  uint8 = 4
)

const InvaildTranscaton types.TransactionId = 0

type Transaction struct {
	Dbid types.DatabaseId
	Xid  types.TransactionId // write xid
	Sid  types.SessionId
}

func NewTranscation(dbid types.DatabaseId, sid types.SessionId) *Transaction {
	return &Transaction{Xid: InvaildTranscaton,
		Dbid: dbid, Sid: sid}
}

func NewTranscationWithXid(dbid types.DatabaseId, xid types.TransactionId, sid types.SessionId) *Transaction {
	return &Transaction{Xid: xid, Dbid: dbid, Sid: sid}
}

// 使用 fdb 的原则， 启动一个事务
func (t *Transaction) Start(manual bool) error {
	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		sessTran := NewSessionOperator(tr, nil)
		err := sessTran.GetSession(t.Sid)
		if err != nil {
			return nil, err
		}
		if sessTran.s.InTransaction {
			return nil, errors.New("session in transaction")
		}
		sessTran.s.InManualTransaction = manual
		sessTran.s.InTransaction = true
		err = sessTran.UpdateSession()
		if err != nil {
			return nil, err
		}

		tick := &kv.SessionTick{Id: t.Sid, LastTick: time.Now().UnixMicro()}
		baseTran := NewKvOperator(tr, tick, tick)
		return nil, baseTran.Write()
	})
	return e
}

// 使用 fdb 的原则，事务不要超过一个函数
func (t *Transaction) AssignReadXid(sessionid types.SessionId, autoCommit bool) error {
	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		st := NewSessionOperator(tr, nil)
		err := st.GetSession(sessionid)
		if err != nil {
			return nil, err
		}
		if !st.s.InManualTransaction {
			return nil, errors.New("")
		}
		if st.s.ReadTranscationId != 0 {
			return nil, errors.New("")
		}
		st.s.InManualTransaction = true

		maxTid := &kv.MaxTid{Max: 0, DbID: st.s.DbId}
		baseTran := NewKvOperator(tr, maxTid, maxTid)
		err = baseTran.Read()
		if err != nil {
			return nil, err
		}

		st.s.ReadTranscationId = maxTid.Max
		t.Xid = maxTid.Max
		return nil, nil
	})
	if e != nil {
		t.Xid = 0
	}
	return e
}

// 使用fdb的原则，事务不要超过一个函数
func (t *Transaction) AssignWriteXid(autoCommit bool) error {
	db := fdb.MustOpenDefault()
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		st := NewSessionOperator(tr, nil)
		err := st.GetSession(t.Sid)
		if err != nil {
			return nil, err
		}
		if !st.s.InManualTransaction {
			return nil, errors.New("")
		}
		if st.s.WriteTranscationId != 0 {
			return nil, errors.New("")
		}
		st.s.InManualTransaction = true

		maxTid := &kv.MaxTid{Max: 0, DbID: st.s.DbId}
		baseTran := NewKvOperator(tr, maxTid, maxTid)
		err = baseTran.Read()
		if err != nil {
			return nil, err
		}
		maxTid.Max += 1
		err = baseTran.Write()
		if err != nil {
			return nil, err
		}

		if st.s.ReadTranscationId == 0 {
			st.s.ReadTranscationId = maxTid.Max
		}
		st.s.WriteTranscationId = maxTid.Max
		t.Xid = maxTid.Max

		kv := &kv.TransactionInfo{Sessionid: t.Sid, Tid: t.Xid, DbId: st.s.DbId,
			Status: XS_START, AutoCommit: autoCommit, UpdateKeySeq: 1}

		baseTran.k, baseTran.v = kv, kv
		err = baseTran.Write()
		if err != nil {
			return nil, err
		}

		return nil, nil
	})
	if e != nil {
		t.Xid = 0
	}
	return e
}

func (t *Transaction) CheckVaild(tr fdb.Transaction) (error, *kv.TransactionInfo) {
	// 保证事务是有效的
	var tkv kv.TransactionInfo
	tkv.Tid = t.Xid
	tkv.DbId = t.Dbid
	baseTran := NewKvOperator(tr, &tkv, &tkv)
	err := baseTran.Read()
	if err != nil {
		return err, nil
	}
	if tkv.Status != XS_START {
		return errors.New(""), nil
	}

	// 保证 session 是有效的
	var session kv.Session
	session.Id = t.Sid

	op := NewSessionOperator(tr, &session)
	if !op.CheckSessionAlive(t.Sid) {
		return errors.New(""), nil
	}

	return nil, &tkv
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

func (t *Transaction) Commit() error {
	// 更新事务状态
	db := fdb.MustOpenDefault()
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		err, tkv := t.CheckVaild(tr)
		if err != nil {
			return nil, err
		}
		tkv.Status = XS_COMMIT
		baseTran := NewKvOperator(tr, tkv, tkv)
		err = baseTran.Write()
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err == nil {
		// 异步更新kv对即可
		go t.CommitKV(XS_COMMIT)
	}
	return err
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
