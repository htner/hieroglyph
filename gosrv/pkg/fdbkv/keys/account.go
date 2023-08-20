package keys

import (
	"bytes"
	"encoding/binary"
)

type SDBAccountKey struct {
	Id uint64
}

func (key *SDBAccountKey) Tag() uint16 {
	return SdbAccountKeyTag
}

func (key *SDBAccountKey) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.BigEndian, key.Id)
}

func (key *SDBAccountKey) DecFdbKey(reader *bytes.Reader) error {
	return binary.Read(reader, binary.BigEndian, &key.Id)
}

// AccountLoginName
type SDBAccountLoginNameKey struct {
	LoginName string
}

func (key *SDBAccountLoginNameKey) Tag() uint16 {
	return AccountLoginNameKeyTag
}

func (key *SDBAccountLoginNameKey) EncFdbKey(buf *bytes.Buffer) error {
	_, err := buf.WriteString(key.LoginName)
	return err
}

func (key *SDBAccountLoginNameKey) DecFdbKey(reader *bytes.Reader) error {
	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	key.LoginName = buf.String()
	return nil
}

// OrganizationKey
type OrganizationKey struct {
	Id uint64
}

func (key *OrganizationKey) Tag() uint16 {
	return OrganizationKeyTag
}

func (key *OrganizationKey) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.BigEndian, key.Id)
}

func (key *OrganizationKey) DecFdbKey(reader *bytes.Reader) error {
	return binary.Read(reader, binary.BigEndian, &key.Id)
}

// UserKey
type UserKey struct {
	Id uint64
}

func (key *UserKey) Tag() uint16 {
	return UserKeyTag
}

func (key *UserKey) EncFdbKey(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.BigEndian, key.Id)
}

func (key *UserKey) DecFdbKey(reader *bytes.Reader) error {
	return binary.Read(reader, binary.BigEndian, &key.Id)
}

// AccountLoginName
type UserLoginNameKey struct {
	OrganizationId uint64
	LoginName      string
}

func (key *UserLoginNameKey) Tag() uint16 {
	return LoginNameKeyTag
}

func (key *UserLoginNameKey) EncFdbKey(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.BigEndian, key.OrganizationId)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(key.LoginName)
	return err
}

func (key *UserLoginNameKey) DecFdbKey(reader *bytes.Reader) error {
	err := binary.Read(reader, binary.BigEndian, &key.OrganizationId)
	if err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	key.LoginName = buf.String()
	return nil
}

// OrganizationNameKey
type OrganizationNameKey struct {
	Name string
}

func (key *OrganizationNameKey) Tag() uint16 {
	return OrganizationNameKeyTag
}

func (key *OrganizationNameKey) EncFdbKey(buf *bytes.Buffer) error {
	_, err := buf.WriteString(key.Name)
	return err
}

func (key *OrganizationNameKey) DecFdbKey(reader *bytes.Reader) error {
	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	key.Name = buf.String()
	return nil
}
