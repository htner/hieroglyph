package keys

import (
	"bytes"
	"encoding/binary"
)

// key database/rel/filename->info
type FileSharedKey struct {
	Fileid   uint64
}

func (*FileSharedKey) Tag() uint16 {
	return LakeFileTag
}

func (file *FileSharedKey) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.BigEndian, file.Fileid)
}

func (file *FileSharedKey) DecFdbKey(reader *bytes.Reader) error {
	return binary.Read(reader, binary.BigEndian, &file.Fileid)
}
