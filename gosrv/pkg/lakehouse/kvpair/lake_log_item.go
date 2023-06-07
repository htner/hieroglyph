package kvpair

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"

	"github.com/htner/sdb/gosrv/pkg/types"
)

const (
	InsertMark    int8 = 1
	DeleteMark    int8 = 2
	PreInsertMark int8 = 3
)

type LakeLogItem struct {
	Database  types.DatabaseId
	Rel       types.RelId
	Xid       types.TransactionId
	Filename  string
	Action    int8
	LinkFiles []string
}

func (s *LakeLogItem) Tag() uint16 {
	return LakeLogTag
}

func (item *LakeLogItem) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, item.Database)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, item.Rel)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, item.Xid)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(item.Filename)
	return err
}

func (item *LakeLogItem) EncFdbValue(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, item.Action)
	if item.Action == PreInsertMark {
		data, err := json.Marshal(item.LinkFiles)
		if err != nil {
			return err
		}
		_, err = buf.Write(data)
		if err != nil {
			return err
		}
	}
	return err
}

func (item *LakeLogItem) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &item.Database)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &item.Rel)
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, &item.Xid)
}

func (item *LakeLogItem) DecFdbValue(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, &item.Action)
	if err != nil {
		return err
	}
	if item.Action == PreInsertMark {
		var bytes []byte
		bytes, err = ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
		err = json.Unmarshal(bytes, &item.LinkFiles)
	}
	return err
}
