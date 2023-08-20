package keys

import (
	"bytes"
	"encoding/binary"
)

// key database/rel/filename->info
type FileKey struct {
	Database uint64
	Relation uint64
	Fileid   uint64
}

func (*FileKey) Tag() uint16 {
	return LakeFileTag
}

func (file *FileKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, file.Database)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, file.Relation)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.BigEndian, file.Fileid)
}

func (file *FileKey) DecFdbKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, &file.Database)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, &file.Relation)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.BigEndian, &file.Fileid)
	if err != nil {
		return err
	}
	return nil
}

func (file *FileKey) RangePerfix(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, file.Database)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, file.Relation)
	if err != nil {
		return err
	}
	if file.Fileid == 0 {
		return nil
	} else {
		return binary.Write(buf, binary.BigEndian, file.Fileid)
	}
}
