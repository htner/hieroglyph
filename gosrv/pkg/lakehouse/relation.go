package lakehouse

import (
	"github.com/htner/sdb/gosrv/pkg/types"
)

type RelNode struct {
	Database types.DatabaseId
	Relation types.RelId
}

type RelDesc struct {
	Database types.DatabaseId
	Id       types.RelId
	Type     types.TypeId
	Kind     types.KindId
	AM       types.OID
}

type Relation struct {
}

func (r *Relation) NewRelation(rel RelDesc, xid types.TransactionId, sid types.SessionId) {
	//t := NewTranscationWithXid(rel.Database, xid, sid) //InitTranscation(xid)
}
