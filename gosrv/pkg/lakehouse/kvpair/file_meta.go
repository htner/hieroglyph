package kvpair

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/htner/sdb/gosrv/pkg/types"
)

// key database/rel/filename->info
type FileMeta struct {
	Database types.DatabaseId
	Relation types.RelId
	FileName string

	Xmin types.TransactionId
	Xmax types.TransactionId
	XminState XState 
	XmaxState XState 
	meta map[string]string
  	Space uint64 
}

func (*FileMeta) Tag() uint16 {
	return LakeFileTag
}


func (file *FileMeta) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, file.Database)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, file.Relation)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(key.FileName)
	return err
}

func (file *FileMeta) DecFdbKey(reader *bytes.Reader) error {
  err := binary.Read(reader, binary.LittleEndian, file.Database)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, file.Relation)
	if err != nil {
		return err
	}
  bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
  key.FileName = string(bytes)
}

/*
func (file *FileMeta) RangePerfix(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, key.Database)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, key.Relation)
	if err != nil {
		return err
	}
	return key.UserKey.EncKey(buf)
}
*/


func (f *FileMeta) EncValue(buf *bytes.Buffer) error {
	encoder := gob.NewEncoder(buf)
	return encoder.Encode(f)
}

func (f *FileMeta) DecValue(buf *bytes.Reader) error {
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(f)
}
