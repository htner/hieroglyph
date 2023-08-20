package lakehouse

import (
	"github.com/htner/sdb/gosrv/pkg/types"
)

type Attribute struct {
	Name   string
	TypeId types.TypeId
}

type PGAttribute struct {
	Attrelid      types.OID
	Attname       string
	Atttypeid     types.OID
	Attstattarget int32
	Attlen        int16
	Attnum        int16
	Attndims      int32
	attcacheoff   int32
	attypemod     int32
	attbyval      bool
	attstorage    byte
	attalign      byte
	attnotnull    bool
	atthasdef     bool
	atthasmissing bool
	attidentity   byte
	attgenerated  byte
	attisdropped  bool
	attislocal    bool
	attinhcount   int32
	attcollation  types.OID
}
