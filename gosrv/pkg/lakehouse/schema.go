package lakehouse

import (
	"github.com/htner/sdb/gosrv/pkg/transaction"
	"github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type Schema struct {
}

func (r *Schema) NewSchema(rel RelNode, attrs []*Attribute, xid types.TransactionId, sid types.SessionId) {
	t := transaction.NewTranscationWithXid(rel.Database, xid, sid)
	kvs := make([]*kvpair.MvccKVPair, 0)
	for _, attr := range attrs {
		k := &kvpair.MvccKey{
			Database: rel.Database,
			Relation: rel.Relation,
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
	t.WriteAndAutoDeletes(kvs)
}

func (r *Schema) NewAttribute(rel RelNode, attr Attribute, xid types.TransactionId, sid types.SessionId) {
	r.UpdateAttribute(rel, attr, xid, sid)
}

func (r *Schema) UpdateAttribute(rel RelNode, attr Attribute, xid types.TransactionId, sid types.SessionId) {
	t := transaction.NewTranscationWithXid(rel.Database, xid, sid)
	k := &kvpair.MvccKey{
		Database: rel.Database,
		Relation: rel.Relation,
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
	t.WriteAndAutoDelete(kv)
}
