package fdbkv

import (
	"errors"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"google.golang.org/protobuf/proto"
)

var ErrEmptyData error = errors.New("empty")

type KvOperator struct {
	t fdb.Transaction
}

func NewKvOperator(tr fdb.Transaction) *KvOperator {
	return &KvOperator{t: tr}
}

func (t *KvOperator) Write(key FdbKey, value FdbValue) error {
	sKey, err := MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	sValue, err := MarshalValue(value)
	log.Println("kv write:", sKey, len(sKey), sValue, len(sValue))
	if err != nil {
		return err
	}
	// log.Println("write ", fKey, sKey, sValue)
	t.t.Set(fKey, sValue)
	return nil
}

func (t *KvOperator) Delete(key FdbKey) error {
	sKey, err := MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	t.t.Clear(fKey)
	return nil
}

func (t *KvOperator) Read(key FdbKey, value FdbValue) error {
	sKey, err := MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	future := t.t.Get(fKey)

	v, e := future.Get()
	// log.Println("kvoprator read, key, value, error:", key, len(sKey), len(v), v, e)
	if e != nil {
		// log.Printf("kv not found")
		return errors.New("kv not found")
	}
	if len(v) == 0 {
		return ErrEmptyData
	}
	return UnmarshalValue(v, value)
}

func (t *KvOperator) WritePB(key FdbKey, msg proto.Message) error {
	sKey, err := MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	sValue, err := proto.Marshal(msg)
	//sValue, err := MarshalValue(value)
	// log.Println(sKey, len(sKey), sValue, len(sValue))
	if err != nil {
		return err
	}
	// log.Println("write ", fKey, sKey, sValue)
	t.t.Set(fKey, sValue)
	return nil
}

func (t *KvOperator) ReadPB(key FdbKey, msg proto.Message) error {
	sKey, err := MarshalKey(key)
	if err != nil {
		return err
	}
	fKey := fdb.Key(sKey)
	future := t.t.Get(fKey)

	v, e := future.Get()
	//log.Println("v e", key, v, e)
	// log.Println("read pb key, value, error:", key, len(sKey), v, len(v), e)
	if e != nil {
		return errors.New("kv not found")
	}
	if len(v) == 0 {
		return ErrEmptyData
	}
	return proto.Unmarshal(v, msg)
}
