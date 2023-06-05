package kvpair

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"

	"github.com/htner/sdb/gosrv/pkg/types"
)

// key database/rel/filename->info
type FileMeta struct {
	Database types.DatabaseId
	Filename string

	Relation  types.RelId
	Xmin      types.TransactionId
	Xmax      types.TransactionId
	XminState types.XState
	XmaxState types.XState
	meta      map[string]string
	Space     uint64
}

func (*FileMeta) Tag() uint16 {
	return LakeFileTag
}

func (file *FileMeta) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, file.Database)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(file.Filename)
	return err
}

func (file *FileMeta) DecFdbKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, file.Database)
	if err != nil {
		return err
	}
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	file.Filename = string(bytes)
	return nil
}

func (file *FileMeta) RangePerfix(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, file.Database)
}

func (file *FileMeta) EncFdbValue(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, file.Relation)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, file.Xmin)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, file.Xmax)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, file.XminState)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, file.XmaxState)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, file.Space)
	if err != nil {
		return err
	}
	dataType, err := json.Marshal(file.meta)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(string(dataType))
	return err
}

func (file *FileMeta) DecFdbValue(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, file.Relation)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, file.Database)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, file.Relation)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, file.Database)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, file.Relation)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, file.Database)
	if err != nil {
		return err
	}
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bytes, &file.meta)
	return err
}
