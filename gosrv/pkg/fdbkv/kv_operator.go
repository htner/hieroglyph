package fdbkv 

import (
	"errors"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"google.golang.org/protobuf/proto"
	kv "github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
)

type KvOperator struct {
	t fdb.Transaction
}

func NewKvOperator(tr fdb.Transaction) *KvOperator {
	return &KvOperator{t: tr}
}

func (t *KvOperator) Write(key kv.FdbKey, value kv.FdbValue) error {
	sKey, err := kv.MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	sValue, err := kv.MarshalValue(value)
	// log.Println(sKey, len(sKey), sValue, len(sValue))
	if err != nil {
		return err
	}
	// log.Println("write ", fKey, sKey, sValue)
	t.t.Set(fKey, sValue)
	return nil
}

func (t *KvOperator) Delete(key kv.FdbKey) error {
	sKey, err := kv.MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	t.t.Clear(fKey)
	return nil
}

func (t *KvOperator) Read(key kv.FdbKey, value kv.FdbValue) error {
	sKey, err := kv.MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	future := t.t.Get(fKey)

	v, e := future.Get()
	log.Println("v e", key, v, e)
	if e != nil {
		return errors.New("kv not found")
	}
	return kv.UnmarshalValue(v, value)
}

func (t *KvOperator) WritePB(key kv.FdbKey, msg proto.Message) error {
	sKey, err := kv.MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
  sValue, err := proto.Marshal(msg)
	//sValue, err := kv.MarshalValue(value)
	// log.Println(sKey, len(sKey), sValue, len(sValue))
	if err != nil {
		return err
	}
	// log.Println("write ", fKey, sKey, sValue)
	t.t.Set(fKey, sValue)
	return nil
}

func (t *KvOperator) ReadPB(key kv.FdbKey, msg proto.Message) error {
	sKey, err := kv.MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	future := t.t.Get(fKey)

	v, e := future.Get()
	log.Println("v e", key, v, e)
	if e != nil {
		return errors.New("kv not found")
	}
	return proto.Unmarshal(v, msg)
}
