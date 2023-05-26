package kvpair

import (
	"bytes"
)

type EmptyFdbValue struct {
}

func (e *EmptyFdbValue) EncFdbValue(*bytes.Buffer) error {
	return nil
}

func (e *EmptyFdbValue) DecFdbValue(*bytes.Reader) error {
	return nil
}
