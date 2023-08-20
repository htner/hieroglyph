package kvpair

import (
	"bytes"
	"encoding/binary"
	"log"

	"github.com/htner/sdb/gosrv/pkg/types"
)

// key database/rel/filename->info
type FileKey struct {
	Database types.DatabaseId
	Relation types.RelId
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
	log.Printf("reader %d", reader.Len())
	err := binary.Read(reader, binary.LittleEndian, &file.Database)
	if err != nil {
		log.Println("reader database error")
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, &file.Relation)
	if err != nil {
		log.Println("reader relation error")
		return err
	}
	err = binary.Read(reader, binary.BigEndian, &file.Fileid)
	if err != nil {
		log.Println("reader fileid error")
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
