package lakehouse

import (
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/utils"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

type LakeRelOperator struct {
	T *Transaction
}

func NewLakeRelOperator(dbid types.DatabaseId, sid types.SessionId, xid types.TransactionId) (L *LakeRelOperator) {
	return &LakeRelOperator{T: NewTranscationWithXid(dbid, xid, sid)}
}

func (L *LakeRelOperator) PrepareFiles(rel types.RelId, files []string) error {
	// 上锁
	db, err := fdb.OpenDefault()
	if err != nil {
		log.Println("PrepareFiles open err: ", err)
		return err
	}
	var mgr LockMgr
	var fdblock Lock
	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = InsertLock
	fdblock.Sid = L.T.Sid

	_, e := mgr.DoWithAutoLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			_, err := t.CheckVaild(tr)
			if err != nil {
				return nil, err
			}
			kvOp := NewKvOperator(tr)
      var prepareFiles sdb.PrepareLakeFiles
      prepareFiles.Filenames = files
      key := kvpair.NewLakeLogItemKey(L.T.Database, rel, L.T.Xid, kvpair.PreInsertMark)
      return nil, kvOp.WritePB(key, &prepareFiles)
		}, 3)
	if e != nil {
		log.Println("PrepareFiles lock err: ", e)
	}
	return e
}

func (L *LakeRelOperator) ChangeFiles(rel types.RelId, insertFiles []*sdb.LakeFileDetail, deleteFiles []*sdb.LakeFileHandle) error {
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
	fdblock.Sid = L.T.Sid

	_, e := mgr.DoWithAutoLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			_, err := t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

			kvOp := NewKvOperator(tr)
      idKey := kvpair.SecondClassObjectMaxKey{MaxTag:kvpair.MAXFILEIDTag, Dbid: L.T.Database}
      idOp := utils.NewMaxIdOperator(tr, &idKey)
      _, err = idOp.GetCurrent()
      if err != nil {
        return nil, err
      }

      //var inserts []*LakeFileDetail 
      var log_details sdb.InsertLakeFiles
      log_details.Files = make([]*sdb.LakeFileDetail, 0)
			for _, file := range insertFiles {
        fileid, err := idOp.GetLocalNext()
        if err != nil {
          return nil, err
        }

        var key kvpair.FileKey
        key.Database = L.T.Database 
        key.Relation = rel  
        key.Fileid = fileid 

        file.BaseInfo.Fileid = fileid
				file.Dbid = uint64(L.T.Database)
				file.Rel = uint64(rel)
				file.Xmin = uint64(t.Xid)
				file.Xmax = uint64(InvaildTranscaton)
				file.XminState = uint32(XS_START)
				file.XmaxState = uint32(XS_NULL)

        log.Println("insert file:", file)
				err = kvOp.WritePB(&key, file)
				if err != nil {
					return nil, err
				}
        log_details.Files = append(log_details.Files, file)
			}

      key := kvpair.NewLakeLogItemKey(L.T.Database, rel, L.T.Xid, kvpair.PreInsertMark)

      err = kvOp.WritePB(key, &log_details)
      if err != nil {
        return nil, err
      }

      err = idOp.Sync()
      if err != nil {
        return nil, err
      }

      var deleteInfo sdb.DeleteLakeFiles
      deleteInfo.Files = deleteFiles

			for _, file := range deleteInfo.Files {

        var key kvpair.FileKey
        key.Database = L.T.Database
        key.Relation = rel
        key.Fileid = file.Id

        var file sdb.LakeFileDetail
				err = kvOp.ReadPB(&key, &file)
        if err != nil {
          return nil, err
        }

				file.Xmax = uint64(t.Xid)
				file.XmaxState = uint32(XS_START)

				kvOp.WritePB(&key, &file)
				if err != nil {
					return nil, err
				}
			}

      log_key := kvpair.NewLakeLogItemKey(L.T.Database, rel, L.T.Xid, kvpair.PreInsertMark)
      kvOp.WritePB(log_key, &deleteInfo)
      if err != nil {
        return nil, err
      }

      changeRelKey := &kvpair.TransactionChangeRelKey{Database: L.T.Database, Rel:rel, Xid:L.T.Xid}
      var emptyValue kvpair.EmptyValue
      err = kvOp.Write(changeRelKey, &emptyValue)
      if err != nil {
        return nil, err
      }
      log.Printf("change files finish")
			return nil, nil
		}, 3)
	return e
}

func (L *LakeRelOperator) InsertFiles(rel types.RelId, files []*sdb.LakeFileDetail) error {
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
	fdblock.Sid = L.T.Sid

	_, e := mgr.DoWithAutoLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			_, err := t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

			kvOp := NewKvOperator(tr)
      idKey := kvpair.SecondClassObjectMaxKey{MaxTag:kvpair.MAXFILEIDTag, Dbid: L.T.Database}
      idOp := utils.NewMaxIdOperator(tr, &idKey)
      _, err = idOp.GetCurrent()
      if err != nil {
        return nil, err
      }

      //var inserts []*LakeFileDetail 
      var log_details sdb.InsertLakeFiles
      log_details.Files = make([]*sdb.LakeFileDetail, 0)
			for _, file := range files {
        fileid, err := idOp.GetLocalNext() 
        if err != nil {
          return nil, err
        }

        var key kvpair.FileKey
        key.Database = L.T.Database 
        key.Relation = rel  
        key.Fileid = fileid 

				file.Dbid = uint64(L.T.Database)
				file.Rel = uint64(rel)
				file.Xmin = uint64(t.Xid)
				file.Xmax = uint64(InvaildTranscaton)
				file.XminState = uint32(XS_START)
				file.XmaxState = uint32(XS_NULL)

				err = kvOp.WritePB(&key, file)
				if err != nil {
					return nil, err
				}
        log_details.Files = append(log_details.Files, file)
			}

      err = idOp.Sync()
      if err != nil {
        return nil, err
      }

      key := kvpair.NewLakeLogItemKey(L.T.Database, rel, L.T.Xid, kvpair.PreInsertMark)

      err = kvOp.WritePB(key, &log_details)
      if err != nil {
        return nil, err
      }

      changeRelKey := &kvpair.TransactionChangeRelKey{Database: L.T.Database, Rel:rel, Xid:L.T.Xid}
      var emptyValue kvpair.EmptyValue
      err = kvOp.Write(changeRelKey, &emptyValue)
      if err != nil {
        return nil, err
      }

			return nil, nil
		}, 3)
	return e
}

func (L *LakeRelOperator) DeleleFiles(rel types.RelId, files []*sdb.LakeFileDetail) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	var mgr LockMgr
	var fdblock Lock
	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = UpdateLock
	fdblock.Sid = L.T.Sid

	_, e := mgr.DoWithAutoLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			_, err := t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

			kvOp := NewKvOperator(tr)
      
      var deleteInfo sdb.DeleteLakeFiles
      deleteInfo.Files = make([]*sdb.LakeFileHandle, 0)

			for _, file := range files {

        var key kvpair.FileKey
        key.Database = L.T.Database
        key.Relation = rel
        key.Fileid = file.BaseInfo.Fileid

				file.Dbid = uint64(L.T.Database)
				file.Rel = uint64(rel)
				file.Xmax = uint64(t.Xid)
				file.XmaxState = uint32(XS_START)

				kvOp.WritePB(&key, file)
				if err != nil {
					return nil, err
				}

        deleteInfo.Files = append(deleteInfo.Files, &sdb.LakeFileHandle{Id: key.Fileid, Name: file.BaseInfo.FileName})
			}

      log_key := kvpair.NewLakeLogItemKey(L.T.Database, rel, L.T.Xid, kvpair.PreInsertMark)
      kvOp.WritePB(log_key, &deleteInfo)
      if err != nil {
        return nil, err
      }

      changeRelKey := &kvpair.TransactionChangeRelKey{Database: L.T.Database, Rel:rel, Xid:L.T.Xid}
      var emptyValue kvpair.EmptyValue
      err = kvOp.Write(changeRelKey, &emptyValue)
      if err != nil {
        return nil, err
      }

			return nil, nil
		}, 3)
	return e
}

func (L *LakeRelOperator) FlushCommit() {
  // TODO
}

func (L *LakeRelOperator) Abrot() {
  // TODO
}
