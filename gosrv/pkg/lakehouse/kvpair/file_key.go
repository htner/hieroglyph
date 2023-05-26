package kvpair

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type FileKey struct {
	tag      uint16
	Database types.DatabaseId
	Relation types.RelId
  FileName string   // uniq for all time in table
}

func (key *FileKey) SetTag(t uint16) {
	key.tag = t
}

func (key *FileKey) Tag() uint16 {
	return key.tag
}

func (key *FileKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, key.Database)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, key.Relation)
	if err != nil {
		return err
	}
  return buf.WriteString(key.FileName)
}

func (key *FileKey) DecFdbKey(reader *bytes.Reader) error {
  err := binary.Read(reader, binary.LittleEndian, key.Database)
	if err != nil {
		return err
	}
	err := binary.Read(reader, binary.LittleEndian, key.Relation)
	if err != nil {
		return err
	}
  bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
  key.FileName = string(bytes)
}

// 拿出 MVCC
func (key *FileKey) RangePerfix(buf *bytes.Buffer) error {
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
