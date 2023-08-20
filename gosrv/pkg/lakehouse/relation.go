package lakehouse

import (
	"github.com/htner/sdb/gosrv/pkg/transaction"
	"github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
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
	t := transaction.NewTranscationWithXid(rel.Database, xid, sid) //InitTranscation(xid)
	kvs := make([]*kvpair.MvccKVPair, 0)
	/*
		for _, attr := range attrs {
			k := &kvpair.MvccKey{
				Database: rel.Database,
				Relation: rel.Id,
				UserKey:  kvpair.NewDefaultKey(attr.Name),
				Xmin:     xid,
			}
			v := &kvpair.MvccValue{
				Xmax:       0,
				XminStatus: 0,
				XmaxStatus: 0,
				UserValue:  kvpair.NewDefaultKey(attr),
			}
			kv := &kvpair.MvccKVPair{K: k, V: v}
			kvs = append(kvs, kv)
		}
	*/
	t.WriteAndAutoDeletes(kvs)
}
