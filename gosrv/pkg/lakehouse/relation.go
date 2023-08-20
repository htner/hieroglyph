package lakehouse

import (
	"github.com/htner/sdb/gosrv/pkg/types"
)

type RelNode struct {
	Database uint64
	Relation uint64
}

type RelDesc struct {
	Database uint64
	Id       uint64
	Type     uint64
	Kind     types.KindId
	AM       types.OID
}

type Relation struct {
}

func (r *Relation) NewRelation(rel RelDesc, xid uint64, sid uint64) {
	//t := NewTranscationWithXid(rel.Database, xid, sid) //InitTranscation(xid)
}
