package kvpair

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type MvccValue struct {
	Xmax       types.TransactionId
	XminStatus uint8
	XmaxStatus uint8
	UserValue  Value
}

func (val *MvccValue) EncFdbValue(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, val.Xmax)
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
	return val.UserValue.EncValue(buf)
}

func (v *MvccValue) DecFdbValue(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, v.Xmax)
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
	return v.UserValue.DecValue(reader)
}
