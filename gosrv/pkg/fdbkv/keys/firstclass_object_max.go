package keys

import (
	"bytes"
)

type FirstClassObjectMaxKey struct {
	MaxTag uint16
}

func (b *FirstClassObjectMaxKey) Tag() uint16 {
	return b.MaxTag
}

func (m *FirstClassObjectMaxKey) EncFdbKey(buf *bytes.Buffer) error {
	return nil
}

func (m *FirstClassObjectMaxKey) DecFdbKey(buf *bytes.Reader) error {
	return nil
}
