package config

import "github.com/htner/sdb/gosrv/proto/sdb"

type LakeSpaceConfig struct {
}

func (c *LakeSpaceConfig) GetConfig(dbid uint64) (*sdb.LakeSpace, error) {
	detail := new(sdb.LakeSpace)
  detail.SpaceId = 1
	detail.S3Info = new(sdb.S3Endpoint)

	detail.S3Info.Bucket = "sdb1"
	detail.S3Info.Region = "ap-northeast-1"
	detail.S3Info.Endpoint = "127.0.0.1:9000"
	detail.S3Info.User = "minioadmin"
	detail.S3Info.Password = "minioadmin"
	detail.S3Info.IsMinio = true

	return detail, nil
}
