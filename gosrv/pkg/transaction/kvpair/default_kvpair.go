package kvpair

import (
	"bytes"
	"encoding/gob"
)

type DefaultKVPair struct {
	data interface{}
}

func NewDefaultKey(data interface{}) *DefaultKVPair {
	d := &DefaultKVPair{data: data}
	return d
}

func (d *DefaultKVPair) EncKey(buf *bytes.Buffer) error {
	encoder := gob.NewEncoder(buf)
	return encoder.Encode(d.data)
}

func (d *DefaultKVPair) DecKey(buf *bytes.Reader) error {
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(d.data)
}

func (d *DefaultKVPair) EncValue(buf *bytes.Buffer) error {
	encoder := gob.NewEncoder(buf)
	return encoder.Encode(d.data)
}

func (d *DefaultKVPair) DecValue(buf *bytes.Reader) error {
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(d.data)
}
