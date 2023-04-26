package kvpair

import "bytes"

type Key interface {
	EncKey(*bytes.Buffer) error
	DecKey(*bytes.Reader) error
}

type Value interface {
	EncValue(*bytes.Buffer) error
	DecValue(*bytes.Reader) error
}
