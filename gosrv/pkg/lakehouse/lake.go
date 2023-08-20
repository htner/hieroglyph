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

func (L *LakeRelOperator) InsertFile(f *File) error {
	// 上锁

	key := new(kvpair.MvccKey)
	key.SetTag(kvpair.LakeFileTag)
	key.Database = L.t.Dbid
	key.Relation = f.Rel
	key.UserKey = f
	//key.Xmin = L.t.Xid

	value := new(kvpair.MvccValue)
	value.Xmax = transaction.InvaildTranscaton
	value.XminStatus = transaction.XS_START
	value.XmaxStatus = transaction.XS_NULL
	value.UserValue = f
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
