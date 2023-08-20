package keys

import (
	"bytes"
	"encoding/binary"
)

type FileListVersion struct {
	DbId    uint64
	Rel     uint64
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
