package kvpair

import (
	"bytes"
	"encoding/binary"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type FileListVersion struct {
	DbId    types.DatabaseId
	Rel     types.RelId
	Version uint64
}

func (V *FileListVersion) Tag() uint16 {
	return FileListVersionTag
}

func (V *FileListVersion) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, V.DbId)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, V.Rel)
}

func (V *FileListVersion) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, V.Version)
}

func (V *FileListVersion) DecFdbKey(buf *bytes.Reader) error {
	err := binary.Read(buf, binary.LittleEndian, &V.DbId)
	if err != nil {
		return err
	}
	return binary.Read(buf, binary.LittleEndian, &V.Rel)
}

func (V *FileListVersion) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &V.Version)
}
