package lakehouse

import (
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	kv "github.com/htner/sdb/gosrv/pkg/lakehouse/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type LakeRelOperator struct {
	T *Transaction
}

func NewLakeRelOperator(dbid types.DatabaseId, sid types.SessionId) (L *LakeRelOperator) {
	return &LakeRelOperator{T: NewTranscation(dbid, sid)}
}

func (L *LakeRelOperator) MarkFiles(rel types.RelId, files []string) error {
	// 上锁
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	var mgr LockMgr
	var fdblock Lock
	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = InsertLock

	_, e := mgr.DoWithLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			_, err := t.CheckVaild(tr)
			if err != nil {
				return nil, err
			}

			kvOp := NewKvOperator(tr)
			for _, file := range files {
				item := &kv.LakeLogItem{
					Database:  t.Database,
					Xid:       t.Xid,
					Filename:  file,
					Action:    kv.PreInsertMark,
					LinkFiles: files,
				}
				err := kvOp.Write(item, item)
				if err != nil {
					return nil, err
				}
			}
			return nil, nil
		}, 3)
	return e
}

func (L *LakeRelOperator) InsertFiles(rel types.RelId, files []*kv.FileMeta) error {
	// 上锁
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	var mgr LockMgr
	var fdblock Lock
	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = InsertLock

	_, e := mgr.DoWithLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			_, err := t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

			kvOp := NewKvOperator(tr)
			for _, file := range files {
				file.Xmin = t.Xid
				file.Xmax = InvaildTranscaton
				file.XminState = XS_START
				file.XmaxState = XS_NULL

				err = kvOp.Write(file, file)

				if err != nil {
					return nil, err
				}

				item := &kv.LakeLogItem{
					Database: file.Database,
					Xid:      t.Xid,
					Filename: file.Filename,
					Action:   kv.InsertMark,
				}
				err = kvOp.Write(item, item)

				if err != nil {
					return nil, err
				}
			}
			return nil, nil
		}, 3)
	return e
}

func (L *LakeRelOperator) DeleleFiles(rel types.RelId, files []*kv.FileMeta) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	var mgr LockMgr
	var fdblock Lock
	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = UpdateLock

	_, e := mgr.DoWithLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			_, err := t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

			kvOp := NewKvOperator(tr)

			for _, file := range files {
				file.Xmax = t.Xid
				file.XmaxState = XS_START

				kvOp.Write(file, file)
				if err != nil {
					return nil, err
				}

				item := &kv.LakeLogItem{
					Database: file.Database,
					Xid:      t.Xid,
					Filename: file.Filename,
					Action:   kv.DeleteMark,
				}
				kvOp.Write(item, item)
				if err != nil {
					return nil, err
				}
			}
			return nil, nil
		}, 3)
	return e
}

func (L *LakeRelOperator) GetAllFileForRead(rel types.RelId, filemeta *kv.FileMeta) ([]*kv.FileMeta, types.TransactionId, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, InvaildTranscaton, err
	}
	var mgr LockMgr
	var fdblock Lock

	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = ReadLock

	var session *kv.Session

	fs, e := mgr.DoWithLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			session, err = t.CheckReadAble(tr)
			if err != nil {
				return nil, err
			}
			var files []*kv.FileMeta
			sKey, err := kv.MarshalRangePerfix(filemeta)
			if err != nil {
				return nil, err
			}
			fKey := fdb.Key(sKey)
			rr := tr.GetRange(fdb.KeyRange{Begin: fKey, End: fdb.Key{0xFF}},
				fdb.RangeOptions{Limit: 10000})
			ri := rr.Iterator()

			// Advance will return true until the iterator is exhausted
			for ri.Advance() {
				file := &kv.FileMeta{}
				data, e := ri.Get()
				if e != nil {
					log.Printf("Unable to read next value: %v\n", e)
					return nil, nil
				}
				err = kv.UnmarshalKey(data.Key, file)
				if err != nil {
					return nil, err
				}
				err = kv.UnmarshalValue(data.Value, file)
				if err != nil {
					return nil, err
				}
			}
			return files, nil
		}, 3)
	// check session mvcc
	if session != nil {

	}
	return fs.([]*kv.FileMeta), InvaildTranscaton, e
}

func (L *LakeRelOperator) GetAllFileForUpdate(rel types.RelId, filemeta *kv.FileMeta) ([]*kv.FileMeta, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	var mgr LockMgr
	var fdblock Lock
	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = UpdateLock

	var session *kv.Session

	fs, err := mgr.DoWithLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			session, err = t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}
			var files []*kv.FileMeta
			sKey, err := kv.MarshalRangePerfix(filemeta)
			if err != nil {
				return nil, err
			}
			fKey := fdb.Key(sKey)
			rr := tr.GetRange(fdb.KeyRange{Begin: fKey, End: fdb.Key{0xFF}},
				fdb.RangeOptions{Limit: 10000})
			ri := rr.Iterator()

			// Advance will return true until the iterator is exhausted
			for ri.Advance() {
				file := &kv.FileMeta{}
				data, e := ri.Get()
				if e != nil {
					log.Printf("Unable to read next value: %v\n", e)
					return nil, nil
				}
				err = kv.UnmarshalKey(data.Key, file)
				if err != nil {
					return nil, err
				}
				err = kv.UnmarshalValue(data.Value, file)
				if err != nil {
					return nil, err
				}
			}
			return files, nil
		}, 3)
	// check session mvcc
	if session != nil {

	}
	return fs.([]*kv.FileMeta), err
}
