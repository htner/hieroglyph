package kvpair

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/htner/sdb/gosrv/pkg/types"
)

type MvccKey struct {
	tag      uint16
	Database types.DatabaseId
	Relation types.RelId
	UserKey  Key
	Xmin     types.TransactionId
	// commit but not delete?
}

type MvccKVPair struct {
	K *MvccKey
	V *MvccValue
}

func (key *MvccKey) SetTag(t uint16) {
	key.tag = t
}

func (key *MvccKey) Tag() uint16 {
	return key.tag
}

func (key *MvccKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, key.Database)
	if err != nil {
		return err
	}
	err = binary.Write(buf, binary.LittleEndian, key.Relation)
	if err != nil {
		return err
	}
	err = key.UserKey.EncKey(buf)
	if err != nil {
		return err
	}
	return binary.Write(buf, binary.LittleEndian, key.Xmin)
}

func (key *MvccKey) DecFdbKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, key.Relation)
	if err != nil {
		return err
	}

	len := reader.Len()
	if len <= 8 {
		return errors.New("not enough data left")
	}
	len = len - 8
	userKeyData := make([]byte, len)
	rlen, err := reader.Read(userKeyData)
	if rlen != len {
		return errors.New("not enough data read")
	}
	readerUKey := bytes.NewReader(userKeyData)
	err = key.UserKey.DecKey(readerUKey)
	if err != nil {
		return err
	}

	err = binary.Read(reader, binary.LittleEndian, key.Xmin)
	if err != nil {
		return err
	}
	return nil
}

// 拿出 MVCC
func (key *MvccKey) RangePerfix(buf *bytes.Buffer) error {
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
