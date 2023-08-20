package keys

import (
	"bytes"
	"encoding/binary"
)

type DatabaseKey struct {
	Id uint64
}

func (key *DatabaseKey) Tag() uint16 {
	return DatabaseKeyTag
}

func (key *DatabaseKey) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.BigEndian, key.Id)
}

func (key *DatabaseKey) DecFdbKey(reader *bytes.Reader) error {
	return binary.Read(reader, binary.BigEndian, &key.Id)
}

type DatabaseNameKey struct {
	OrganizationId uint64
	DatabaseName   string
}

func (key *DatabaseNameKey) Tag() uint16 {
	return DatabaseNameKeyTag
}

func (key *DatabaseNameKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.BigEndian, key.OrganizationId)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(key.DatabaseName)
	return err
}

func (key *DatabaseNameKey) DecFdbKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.BigEndian, &key.OrganizationId)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	key.DatabaseName = buf.String()
	return nil
}
