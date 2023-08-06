package fdbkv 

import (
	"errors"

	//_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
  //log "github.com/sirupsen/logrus"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"google.golang.org/protobuf/proto"
	kv "github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
)

type KvReader struct {
	t fdb.ReadTransaction
}

func NewKvReader(tr fdb.ReadTransaction) *KvReader {
	return &KvReader{t: tr}
}

func (t *KvReader) Read(key kv.FdbKey, value kv.FdbValue) error {
	sKey, err := kv.MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	future := t.t.Get(fKey)

	v, e := future.Get()
  //log.Println("key, value, error:", key, len(key), len(v), v, e)
	if e != nil {
    // log.Printf("kv not found")
		return errors.New("kv not found")
	}
  if len(v) == 0 {
    return EmptyDataErr 
  }
	return kv.UnmarshalValue(v, value)
}

func (t *KvReader) ReadPB(key kv.FdbKey, msg proto.Message) error {
	sKey, err := kv.MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	future := t.t.Get(fKey)

	v, e := future.Get()
	//log.Println("v e", key, v, e)
  // log.Println("kvreader read pb key, value, error:", key, len(sKey), v, len(v), e)
	if e != nil {
		return errors.New("kv not found")
	}
  if len(v) == 0 {
    return EmptyDataErr 
  }
	return proto.Unmarshal(v, msg)

}
