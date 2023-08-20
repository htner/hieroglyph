package kvpair

import (
	"bytes"
)

type BytesKVPair struct {
	data []byte
}

func NewBytesKey(data []byte) *BytesKVPair {
	d := &BytesKVPair{data: data}
	return d
}

func (d *BytesKVPair) EncKey(buf *bytes.Buffer) error {
	_, err := buf.Write(d.data)
	return err
}

func (d *BytesKVPair) DecKey(buf *bytes.Reader) error {
	return nil
}

func (d *BytesKVPair) EncValue(buf *bytes.Buffer) error {
	return nil
}

func (d *BytesKVPair) DecValue(buf *bytes.Reader) error {
	return nil
}
