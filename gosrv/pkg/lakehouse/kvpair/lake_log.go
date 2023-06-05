package kvpair

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type LakeLog struct {
	database types.DatabaseId
	xid      types.TransactionId
	Inserts  []string `json:"inserts"`
	Deletes  []string `json:"deletes"`
}

func (s *LakeLog) Tag() uint16 {
	return LakeLogTag
}

func (lakeLog *LakeLog) AppendItem(item *LakeLogItem) {
	if item.Action == InsertMark {
		lakeLog.Inserts = append(lakeLog.Inserts, item.Filename)
	} else if item.Action == DeleteMark {
		lakeLog.Deletes = append(lakeLog.Deletes, item.Filename)
	}
}

func (lakeLog *LakeLog) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, lakeLog.database)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, lakeLog.xid)
}

func (lakeLog *LakeLog) EncFdbValue(buf *bytes.Buffer) error {
	data, err := json.Marshal(lakeLog)
	if err != nil {
		return err
	}
	_, err = buf.Write(data)
	return err
}

func (lakeLog *LakeLog) DecFdbKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, &lakeLog.database)
	if err != nil {
		return err
	}
	return binary.Read(reader, binary.LittleEndian, &lakeLog.xid)
}

func (lakeLog *LakeLog) DecFdbValue(reader *bytes.Reader) error {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bytes, &lakeLog)
	return err
}
