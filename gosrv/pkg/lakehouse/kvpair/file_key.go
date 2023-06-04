package kvpair

import (
	"bytes"
	"encoding/binary"
	"errors"
	"ioutil"
	"github.com/htner/sdb/gosrv/pkg/types"
)

type FileKey struct {
	Database types.DatabaseId
	Relation types.RelId
	FileName string
}

