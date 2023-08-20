package lakehouse

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/pkg/utils"
	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	"github.com/htner/sdb/gosrv/proto/sdb"
	log "github.com/sirupsen/logrus"
)

type LakeRelOperator struct {
	T *Transaction
}

func NewLakeRelOperator(dbid types.DatabaseId, sid types.SessionId, xid types.TransactionId) (L *LakeRelOperator) {
	return &LakeRelOperator{T: NewTranscationWithXid(dbid, xid, sid)}
}

func (L *LakeRelOperator) PrepareFiles(rel types.RelId, count uint64) (files []*sdb.LakeFile, err error) {
	// 上锁
	db, err := fdb.OpenDefault()
	if err != nil {
		log.Println("PrepareFiles open err: ", err)
		return nil, err
	}
	var mgr LockMgr
	var fdblock Lock
	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = InsertLock
	fdblock.Sid = L.T.Sid

	files = make([]*sdb.LakeFile, 0)

	_, e := mgr.DoWithAutoLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			_, err := t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

			kvOp := NewKvOperator(tr)
			idKey := kvpair.SecondClassObjectMaxKey{MaxTag: kvpair.MAXFILEIDTag, Dbid: L.T.Database}
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

				var key kvpair.FileKey
				key.Database = L.T.Database
				key.Relation = rel
				key.Fileid = fileid

				file.BaseInfo = new(sdb.LakeFile)
				file.BaseInfo.FileId = fileid

				file.Dbid = uint64(L.T.Database)
				file.Rel = uint64(rel)
				file.Xmin = uint64(t.Xid)
				file.Xmax = uint64(InvaildTranscaton)
				file.XminState = uint32(XS_START)
				file.XmaxState = uint32(XS_NULL)

				log.Println("insert file:", file)
				err = kvOp.WritePB(&key, &file)
				if err != nil {
					return nil, err
				}
				log_details.Files = append(log_details.Files, &file)

				files = append(files, file.BaseInfo)
			}

			key := kvpair.NewLakeLogItemKey(L.T.Database, rel, L.T.Xid, kvpair.PreInsertMark)

			err = kvOp.WritePB(key, &log_details)
			return nil, err
		}, 3)
	if e != nil {
		log.Println("PrepareFiles lock err: ", e)
	}
	return files, e
}

func (L *LakeRelOperator) DeleteFiles(rel types.RelId, deleteFiles []*sdb.LakeFile) error {
	// 上锁
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
			kvOp := NewKvOperator(tr)
			t := L.T
			_, err := t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

			var deleteInfo sdb.DeleteLakeFiles
			deleteInfo.Files = deleteFiles

			for _, file := range deleteInfo.Files {
				var key kvpair.FileKey
				key.Database = L.T.Database
				key.Relation = rel
				key.Fileid = file.FileId

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

			log_key := kvpair.NewLakeLogItemKey(L.T.Database, rel, L.T.Xid, kvpair.DeleteMark)
			kvOp.WritePB(log_key, &deleteInfo)
			if err != nil {
				return nil, err
			}
			log.Printf("delete files finish")
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
