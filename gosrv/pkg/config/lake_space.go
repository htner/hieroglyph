package config

import "github.com/htner/sdb/gosrv/proto/sdb"

type LakeSpaceConfig struct {

}

func (c *LakeSpaceConfig) GetConfig(dbid uint64) (*sdb.LakeSpaceDetail, error){
  detail := new(sdb.LakeSpaceDetail)
  detail.Base = new(sdb.LakeSpaceInfo)
  detail.Detail = new(sdb.S3Endpoint)
  
  detail.Base.ZoneId = 1
  detail.Base.Bucket = "sdb1"

  detail.Detail.Region = "ap-northeast-1"
  detail.Detail.Endpoint = "127.0.0.1:9000"
  detail.Detail.User = "minioadmin"
  detail.Detail.Password = "minioadmin"
  detail.Detail.IsMinio = true

  return detail, nil
}

