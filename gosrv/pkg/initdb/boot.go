package main

import (
	"bytes"
	"encoding/csv"
	"log"

	//"github.com/htner/sdb/gosrv/catalog"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/transaction"
	"github.com/htner/sdb/gosrv/pkg/transaction/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/pkg/utils/slog"
)

// type OID uint64
type TransactionId types.TransactionId
type MultiXactId types.TransactionId

const PG_CATALOG_NAMESPACE = 11
const MAXATTR = 100

// MaxUint is the maximum value of an uint.
const MaxUint = ^uint(0)

// MaxInt is the maximum value of an int.
const MaxInt = int(MaxUint >> 1)

//const InvalidOid = 0

var num_columns_read int

const BOOTCOL_NULL_AUTO = 1
const BOOTCOL_NULL_FORCE_NULL = 2
const BOOTCOL_NULL_FORCE_NOT_NULL = 3

type StatementUnion struct {
	val interface{}
}

type Bootstrap struct {
	curRel         string
	curDBOid       types.DatabaseId
	curRelOid      types.RelId
	curRow         Row
	curAttr        []*lakehouse.Attribute
	curIndex       uint64
	curPgTypeReady bool
	//bootTypeInfos TypeInfos
}

func (b *Bootstrap) OpenRelation(relName string) {
	b.curRel = relName
}

func (b *Bootstrap) CreateTupleDesc() {
}

func (b *Bootstrap) AppendValueToRow(val string) {
	b.curRow.AddOneValue(val)
}

func (b *Bootstrap) AppendNullToRow() {
	b.curRow.AddOneNull()
}

func (b *Bootstrap) TypeNameToId(typeName string) types.TypeId {
	return 1
}
func (b *Bootstrap) DefineAttr(name, typeName string) {
	if len(b.curAttr)+1 > MAXATTR {
		//elog(FATAL, "too many columns")
		return
	}

	var attr lakehouse.Attribute
	attr.Name = name
	attr.TypeId = b.TypeNameToId(typeName)
	b.curAttr = append(b.curAttr, &attr)
}

func (b *Bootstrap) ResetInsert() {
	b.curRow.ResetRow()
}

func (b *Bootstrap) BootOpenrel(rel string) {
	b.curRel = rel
	log.Println(rel)
}

func (b *Bootstrap) CreateRelation(relname string,
	relnamespace types.OID,
	relid types.RelId,
	accessmtd types.OID,
	relfilenode types.OID,
	reltablespace types.OID,
	shared_relation bool,
	mapped_relation bool,
	relpersistence byte,
	relkind types.KindId) {

	var relDesc lakehouse.RelDesc
	relDesc.Database = types.DatabaseId(b.curDBOid)
	relDesc.Id = relid
	relDesc.Type = 0
	relDesc.Kind = relkind

	relDesc.AM = accessmtd

	//relDesc.Namespace = relnamespace
	//relDesc.Oid = relid
	//relDesc.Tablespace = reltablespace
	//relDesc.Isshared = shared_relation
	//relDesc.Persistence = relpersistence

	// if b.curRel == "pg_class" {
	// if create relation
	// set to catalog
	// return
	// }
	// catalog.NewRelation()
	// var rel catalog.Relation
	// rel.NewRelation(relDesc, b.curAttr, 0)
	if b.curRel == "pg_type" {
		slog.DefaultLogger.Info("using pg_type data")
		b.curPgTypeReady = true
	}
	var relation lakehouse.Relation
	relation.NewRelation(relDesc, 1, 1)

	var rel lakehouse.RelNode
	rel.Database = boot.curDBOid
	rel.Relation = boot.curRelOid
	schema := new(lakehouse.Schema)
	schema.NewSchema(rel, b.curAttr, 1, 1)
}

var boot Bootstrap

type Row struct {
	fields []string
}

func (row *Row) ResetRow() {
	row.fields = row.fields[:0]
}

func (row *Row) AddOneValue(value string) {
	row.fields = append(row.fields, value)
}

func (row *Row) AddOneNull() {
	row.fields = append(row.fields, "null")
	//fmt.Println(nil, i)
}

func (row *Row) BootInsert() {
	//fmt.Println("curRel", "->", row.fields)

	buff := new(bytes.Buffer)
	csvWriter := csv.NewWriter(buff)
	//写入表头
	csvWriter.Write(row.fields)
	//将缓冲区数据写入
	csvWriter.Flush()
	csvBytes := buff.Bytes()

	t := transaction.NewTranscation(1, 1)
	boot.curIndex++
	k := &kvpair.MvccKey{
		Database: boot.curDBOid,
		Relation: boot.curRelOid,
		UserKey:  kvpair.NewDefaultKey(boot.curIndex),
		Xmin:     1,
	}
	k.SetTag(kvpair.SysCatalogTag)

	v := &kvpair.MvccValue{
		Xmax:       transaction.InvaildTranscaton,
		XminStatus: transaction.XS_COMMIT,
		XmaxStatus: transaction.XS_NULL,
		UserValue:  kvpair.NewDefaultKey(csvBytes),
	}
	kv := &kvpair.MvccKVPair{K: k, V: v}
	t.WriteAndAutoDelete(kv)
}
