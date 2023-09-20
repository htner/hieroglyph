package lakehouse

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	"github.com/htner/sdb/gosrv/pkg/utils"
	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	"github.com/htner/sdb/gosrv/proto/sdb"
	log "github.com/sirupsen/logrus"
)

var SUPER_SESSION uint64 = 1
type LakeRelOperator struct {
	T *Transaction
  isSuper bool
}

func NewLakeRelOperator(dbid uint64, sid uint64) (L *LakeRelOperator) {
  l := &LakeRelOperator{T: NewTranscation(dbid, sid)}
  if sid == SUPER_SESSION  {
    l.isSuper = true  
  } else {
    l.isSuper = false
  }
  return l
}

func (L *LakeRelOperator) PrepareFiles(rel uint64, count uint64) (files []*sdb.LakeFile, err error) {
	// 上锁
	db, err := fdb.OpenDefault()
	if err != nil {
		log.Println("PrepareFiles open err: ", err)
		return nil, err
	}
	/*
		var mgr LockMgr
		var fdblock Lock
		fdblock.Database = L.T.Database
		fdblock.Relation = rel
		fdblock.LockType = InsertLock
		fdblock.Sid = L.T.Sid
	*/

	files = make([]*sdb.LakeFile, 0)

	_, e := db.Transact(
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			err := t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

			kvOp := NewKvOperator(tr)
			idKey := keys.SecondClassObjectMaxKey{MaxTag: keys.MAXFILEIDTag, Dbid: 0}
			idOp := utils.NewMaxIdOperator(tr, &idKey)
			start, end, err := idOp.Add(count)
			if err != nil {
				return nil, err
			}

			var log_details sdb.InsertLakeFiles
			log_details.Files = make([]*sdb.LakeFileDetail, 0)
			for fileid := start; fileid <= end; fileid++ {
				fileid, err := idOp.GetLocalNext()
				if err != nil {
					return nil, err
				}

				var file sdb.LakeFileDetail

				var key keys.FileKey
				key.Database = L.T.Database
				key.Relation = rel
				key.Fileid = fileid

				file.BaseInfo = new(sdb.LakeFile)
				file.BaseInfo.FileId = fileid

				file.Dbid = uint64(L.T.Database)
				file.Rel = uint64(rel)
				file.Xmin = uint64(t.session.WriteTransactionId)
				file.Xmax = uint64(InvaildTranscaton)
				file.XminState = uint32(XS_START)
				file.XmaxState = uint32(XS_NULL)
        file.IsShared = false 

				log.Println("insert file:", file)
				err = kvOp.WritePB(&key, &file)
				if err != nil {
					return nil, err
				}
				log_details.Files = append(log_details.Files, &file)

				files = append(files, file.BaseInfo)
			}

			key := keys.NewLakeLogItemKey(L.T.Database, rel, t.session.WriteTransactionId, keys.PreInsertMark)

			err = kvOp.WritePB(key, &log_details)
			return nil, err
		})
	if e != nil {
		log.Println("PrepareFiles lock err: ", e)
	}
	return files, e
}

func (L *LakeRelOperator) DeleteFiles(rel uint64, deleteFiles []*sdb.LakeFile) error {
	// 上锁
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	/*
		var mgr LockMgr
		var fdblock Lock
		fdblock.Database = L.T.Database
		fdblock.Relation = rel
		fdblock.LockType = UpdateLock
		fdblock.Sid = L.T.Sid
	*/

	_, e := db.Transact(
		func(tr fdb.Transaction) (interface{}, error) {
			kvOp := NewKvOperator(tr)
			t := L.T
			err := t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

			var deleteInfo sdb.DeleteLakeFiles
			deleteInfo.Files = deleteFiles

			for _, file := range deleteInfo.Files {
				var key keys.FileKey
				key.Database = L.T.Database
				key.Relation = rel
				key.Fileid = file.FileId

				var file sdb.LakeFileDetail
				err = kvOp.ReadPB(&key, &file)
				if err != nil {
					return nil, err
				}

				file.Xmax = uint64(t.session.WriteTransactionId)
				file.XmaxState = uint32(XS_START)

				kvOp.WritePB(&key, &file)
				if err != nil {
					return nil, err
				}
			}

			log_key := keys.NewLakeLogItemKey(L.T.Database, rel, t.session.WriteTransactionId, keys.DeleteMark)
			kvOp.WritePB(log_key, &deleteInfo)
			if err != nil {
				return nil, err
			}
			log.Printf("delete files finish")
			return nil, nil
		})
	return e
}

func (L *LakeRelOperator) FlushCommit() {
	// TODO
}

func (L *LakeRelOperator) Abrot() {
	// TODO
}
