package lakehouse

import (
  "github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

type LakeOperator struct {
}

func (L *LakeOperator) Copy(dest, source types.DatabaseId) {

}

type LakeDBCopyOperator struct {
  dest types.DatabaseId
  source types.DatabaseId
}

type LakeRelCopyOperator struct {
  destRel types.RelId 
  destDb types.DatabaseId
  destSpace *sdb.LakeSpace 

  sourceRel types.RelId
  sourceDb types.RelId
  sourceSpace *sdb.LakeSpace 

  isSameOrginaztion bool
}

func (L *LakeRelCopyOperator) Copy() {
  
}
