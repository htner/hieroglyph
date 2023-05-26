package lakehouse

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/htner/sdb/gosrv/pkg/types"
)

// key database/rel/filename->info
type File struct {
	Xmin types.TransactionId
	Xmax types.TransactionId
  XminState XState 
  XmaxState XState 
	meta map[string]string
  Space uint64 
}

func (f *File) EncKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, f.Db)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, f.Rel)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(f.name)
	return err
}

func (f *File) EncValue(buf *bytes.Buffer) error {
	encoder := gob.NewEncoder(buf)
	return encoder.Encode(f)
}

func (f *File) DecKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, &f.Db)
	if err != nil {
		return err
	}
	err = binary.Read(reader, binary.LittleEndian, &f.Rel)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	f.name = buf.String()
	return nil
}

func (f *File) DecValue(buf *bytes.Reader) error {
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(f)
}
