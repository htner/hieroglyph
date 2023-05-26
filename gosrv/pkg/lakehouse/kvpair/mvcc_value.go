package kv_pair 

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type FileValue struct {
	Xmin       types.TransactionId
	Xmax       types.TransactionId
	XminStatus uint8
	XmaxStatus uint8
}

func (val *FileValue) EncFdbValue(buf *bytes.Buffer) error {
  err := binary.Write(buf, binary.LittleEndian, val.Xmin)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, val.Xmax)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, val.XminStatus)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, val.XmaxStatus)
	if err != nil {
		return err
	}
  return nil
}

func (v *FileValue) DecFdbValue(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, v.Xmin)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, v.Xmax)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, v.XminStatus)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, v.XmaxStatus)
	if err != nil {
		return err
	}
	len := reader.Len()
	if len <= 0 {
		return errors.New("not enough data left")
	}
	/*
		v.UserValue = make([]byte, len)
		rlen, err := reader.Read(v.UserValue)
		if rlen != len {
			return errors.New("not enough data read")
		}
	*/
  return nil
}
