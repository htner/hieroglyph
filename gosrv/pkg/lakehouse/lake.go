package lakehouse

import (
	"errors"
	"log"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/utils"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/proto/sdb"
	"google.golang.org/protobuf/proto"
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

func (L *LakeRelOperator) GetAllFileForRead(rel types.RelId) ([]*sdb.LakeFileDetail, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	var mgr LockMgr
	var fdblock Lock

	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = ReadLock
	fdblock.Sid = L.T.Sid

	var session *kvpair.Session

	data, err := mgr.DoWithAutoLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			session, err = t.CheckReadAble(tr)
			if err != nil {
        log.Printf("check read able error %v", err)
				return nil, err
			}

      var key kvpair.FileKey = kvpair.FileKey{Database: L.T.Database, Relation: rel, Fileid: 0}

			sKeyStart, err := kvpair.MarshalRangePerfix(&key)
			if err != nil {
        log.Printf("marshal ranage perfix %v", err)
				return nil, err
			}
      key.Fileid = math.MaxUint64 
      sKeyEnd, err := kvpair.MarshalRangePerfix(&key)
			if err != nil {
        log.Printf("marshal ranage perfix %v", err)
				return nil, err
			}

			keyStart := fdb.Key(sKeyStart)
			keyEnd := fdb.Key(sKeyEnd)
			rr := tr.GetRange(fdb.KeyRange{Begin:keyStart, End: keyEnd},
				fdb.RangeOptions{Limit: 10000})
			ri := rr.Iterator()

			// Advance will return true until the iterator is exhausted
			files := make([]*sdb.LakeFileDetail, 0)
			for ri.Advance() {
				file := &sdb.LakeFileDetail{}
				data, e := ri.Get()
				if e != nil {
					log.Printf("Unable to read next value: %v\n", e)
					return nil, nil
				}
        var key kvpair.FileKey
				err = kvpair.UnmarshalKey(data.Key, &key)
				if err != nil {
          log.Printf("UnmarshalKey error ? %v %v", data, err)
					return nil, err
				}
        proto.Unmarshal(data.Value, file)
				if err != nil {
          log.Printf("Unmarshal error %v", err)
					return nil, err
				}
        files = append(files, file)
			}

			return files, nil
		}, 3)

	if err != nil {
		return nil, err
	}

	if data == nil || session == nil {
		return nil, errors.New("data is null")
	}

	files := data.([]*sdb.LakeFileDetail)
	// check session mvcc
	return L.SatisfiesMvcc(files, session.WriteTranscationId)
}

func (L *LakeRelOperator) GetAllFileForUpdate(rel types.RelId) ([]*sdb.LakeFileDetail, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	var mgr LockMgr
	var fdblock Lock
	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = UpdateLock
	fdblock.Sid = L.T.Sid

	var session *kvpair.Session

	data, err := mgr.DoWithAutoLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			session, err = t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

      var key kvpair.FileKey = kvpair.FileKey{Database: L.T.Database, Relation: rel, Fileid: 0}

			sKey, err := kvpair.MarshalRangePerfix(&key)
			if err != nil {
				return nil, err
			}
	
			fKey := fdb.Key(sKey)
			rr := tr.GetRange(fdb.KeyRange{Begin: fKey, End: fdb.Key{0xFF}},
				fdb.RangeOptions{Limit: 10000})
			ri := rr.Iterator()


			// Advance will return true until the iterator is exhausted
			files := make([]*sdb.LakeFileDetail, 0)
			for ri.Advance() {
				file := &sdb.LakeFileDetail{}
				data, e := ri.Get()
				if e != nil {
					log.Printf("Unable to read next value: %v\n", e)
					return nil, nil
				}
				err = kvpair.UnmarshalKey(data.Key, &key)
				if err != nil {
					return nil, err
				}
				err = proto.Unmarshal(data.Value, file)
				if err != nil {
					return nil, err
				}
        files = append(files, file)
			}

			return files, nil
		}, 3)
	if err != nil || err == nil {
		return nil, err
	}
	files := data.([]*sdb.LakeFileDetail)
	// check session mvcc
	return L.SatisfiesMvcc(files, session.WriteTranscationId)
}

func (L *LakeRelOperator) SatisfiesMvcc(files []*sdb.LakeFileDetail, currTid types.TransactionId) ([]*sdb.LakeFileDetail, error){
	satisfiesFiles := make([]*sdb.LakeFileDetail, 0)
  db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
  _, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
    for _, file := range files {
      if uint64(currTid) == file.Xmax {
        continue
      }

      xminState := file.XminState
      xmaxState := file.XmaxState
      kvReader := fdbkv.NewKvReader(rtr)
      // TODO 性能优化
      {
        var minClog kvpair.TransactionCLog
        minClog.Tid = types.TransactionId(file.Xmin)
        minClog.DbId = L.T.Database 
        err := kvReader.Read(&minClog, &minClog)
        if err != nil {
          return nil, errors.New("read error")
        }
        log.Println("file min ", minClog)
        xminState = uint32(minClog.Status)
      }

      if file.Xmax != uint64(InvaildTranscaton) {
        var maxClog kvpair.TransactionCLog
        maxClog.Tid = types.TransactionId(file.Xmax)
        maxClog.DbId = L.T.Database 
        err := kvReader.Read(&maxClog, &maxClog)
        if err != nil {
          return nil, errors.New("read error")
        }
        log.Println("file min ", maxClog)
        xmaxState = uint32(maxClog.Status)
      }

      if xminState == uint32(XS_COMMIT) && xmaxState != uint32(XS_COMMIT) {
        satisfiesFiles = append(satisfiesFiles, file)
        continue
      }
      if uint64(currTid) == file.Xmin {
        satisfiesFiles = append(satisfiesFiles, file)
        continue
      }
    }
    return nil, nil
  })
  if e != nil {
    log.Printf("error %s", e.Error())
    return nil, e
  }
  return satisfiesFiles, nil
}

func (L *LakeRelOperator) FlushCommit() {
  // TODO
}

func (L *LakeRelOperator) Abrot() {
  // TODO
}
