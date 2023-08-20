package keys

/*
	"bytes"
	"encoding/binary"

	"github.com/htner/sdb/gosrv/pkg/types"
*/

/*
type MaxDeleteVersionTag struct {
	DbId uint64
	Rel uint64
	Version uint64
}

func (m *MaxDeleteVersionTag) Tag() uint16 {
	return MaxDeleteVersionTag
}

func (m *MaxDeleteVersionTag) EncFdbKey(buf *bytes.Buffer) error {
  err := binary.Write(buf, binary.LittleEndian, m.DbId)
  if err != nil {
    return err
  }
  return binary.Write(buf, binary.LittleEndian, m.Rel)
}

func (m *MaxDeleteVersionTag) EncFdbValue(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, m.Version)
}

func (m *MaxDeleteVersionTag) DecFdbKey(buf *bytes.Reader) error {
  err := binary.Read(buf, binary.LittleEndian, &m.DbId)
  if err != nil {
    return err
  }
  return binary.Read(buf, binary.LittleEndian, &m.Rel)
}

func (m *MaxDeleteVersionTag) DecFdbValue(buf *bytes.Reader) error {
	return binary.Read(buf, binary.LittleEndian, &m.Version)
}
*/
