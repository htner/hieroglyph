package fdbkv

import (
	"bytes"
)

type EmptyValue struct {
}

func (e *EmptyValue) EncFdbValue(*bytes.Buffer) error {
	return nil
}

func (e *EmptyValue) DecFdbValue(*bytes.Reader) error {
	return nil
}
