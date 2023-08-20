package fdbkv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
)

type FdbKey interface {
	Tag() uint16
	EncFdbKey(*bytes.Buffer) error
	DecFdbKey(*bytes.Reader) error
}

type FdbRangeKey interface {
	Tag() uint16
	RangePerfix(*bytes.Buffer) error
}

type FdbValue interface {
	EncFdbValue(*bytes.Buffer) error
	DecFdbValue(*bytes.Reader) error
}

func MarshalKey(key FdbKey) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, key.Tag())
	if err != nil {
		return nil, err
	}
	err = key.EncFdbKey(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil

}

func MarshalRangePerfix(key FdbRangeKey) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, key.Tag())
	if err != nil {
		return nil, err
	}
	err = key.RangePerfix(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil

}

func UnmarshalKey(data []byte, key FdbKey) error {
	var tag uint16
	reader := bytes.NewReader(data)
	err := binary.Read(reader, binary.LittleEndian, &tag)
	if err != nil {
		log.Printf("read tag failure %d", reader.Len())
		return err
	}
	if tag != key.Tag() {
		log.Printf("read tag mismatch %d-%d", tag, key.Tag())
		return errors.New("")
	}
	return key.DecFdbKey(reader)
}

func MarshalValue(value FdbValue) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := value.EncFdbValue(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalValue(data []byte, value FdbValue) error {
	reader := bytes.NewReader(data)
	return value.DecFdbValue(reader)
}
