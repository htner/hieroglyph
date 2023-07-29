package lakehouse

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/pkg/utils"
	"github.com/htner/sdb/gosrv/pkg/utils/postgres"
	"github.com/htner/sdb/gosrv/proto/sdb"
	"google.golang.org/protobuf/proto"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type LakeOperator struct {
}

func (L *LakeOperator) Copy(dest, source types.DatabaseId) {
  // only catatlog copy now
  var space sdb.LakeSpaceDetail
  space.Base = new(sdb.LakeSpaceInfo)
  space.Detail = new(sdb.S3Endpoint)

  space.Base.Bucket = "sdb1"
  space.Detail.User = "minioadmin"
  space.Detail.Password = "minioadmin"
  space.Detail.IsMinio = true

  // "minioadmin", "minioadmin", true, "127.0.0.1:9000", "ap-northeast-1");

  catalogs := postgres.CatalogNames
  for rel, _:= range catalogs {
    relCopy := new(LakeRelCopyOperator)
    relCopy.destDb = dest
    relCopy.destRel = types.RelId(rel)
    relCopy.destSpace = &space

    relCopy.sourceDb = source
    relCopy.sourceRel = types.RelId(rel)
    relCopy.sourceSpace = &space

    relCopy.Copy()
  }
}

type LakeDBCopyOperator struct {
  dest types.DatabaseId
  source types.DatabaseId
}

type LakeRelCopyOperator struct {
  destRel types.RelId 
  destDb types.DatabaseId
  destSpace *sdb.LakeSpaceDetail

  sourceRel types.RelId
  sourceDb types.DatabaseId
  sourceSpace *sdb.LakeSpaceDetail 

  isSameOrginaztion bool
}

func (L *LakeRelCopyOperator) Copy() error {
  db, err := fdb.OpenDefault()
  if err != nil {
    return err
  }
  var mgr LockMgr
  var fdblock Lock

  fdblock.Database = types.DatabaseId(L.sourceDb) 
  fdblock.Relation = L.sourceRel
  fdblock.LockType = ReadLock
  fdblock.Sid = 1 

  data, err := mgr.DoWithAutoLock(db, &fdblock,
    func(tr fdb.Transaction) (interface{}, error) {

      var key kvpair.FileKey = kvpair.FileKey{Database: L.sourceDb, Relation: L.sourceRel, Fileid: 0}

      sKeyStart, err := kvpair.MarshalRangePerfix(&key)
      if err != nil {
        log.Printf("marshal ranage perfix %v", err)
        return nil, err
      }
      key.Fileid = math.MaxUint64 
      sKeyEnd, err := kvpair.MarshalRangePerfix(&key)
      if err != nil {
        log.Printf("marshal ranage perfix %v", err)
        return nil, err
      }

      keyStart := fdb.Key(sKeyStart)
      keyEnd := fdb.Key(sKeyEnd)
      rr := tr.GetRange(fdb.KeyRange{Begin:keyStart, End: keyEnd},
        fdb.RangeOptions{Limit: 10000})
      ri := rr.Iterator()

      // Advance will return true until the iterator is exhausted
      files := make([]*sdb.LakeFileDetail, 0)
      for ri.Advance() {
        file := &sdb.LakeFileDetail{}
        data, e := ri.Get()
        if e != nil {
          log.Printf("Unable to read next value: %v\n", e)
          return nil, nil
        }
        var key kvpair.FileKey
        err = kvpair.UnmarshalKey(data.Key, &key)
        if err != nil {
          log.Printf("UnmarshalKey error ? %v %v", data, err)
          return nil, err
        }
        proto.Unmarshal(data.Value, file)
        if err != nil {
          log.Printf("Unmarshal error %v", err)
          return nil, err
        }
        files = append(files, file)
      }
      return files, nil
    }, 3)

  if err != nil {
    return err
  }

  if data == nil {
    return errors.New("data is null")
  }

  copys := make(map[string]string, 0)

  data, err = mgr.DoWithAutoLock(db, &fdblock,
    func(tr fdb.Transaction) (interface{}, error) {
      kvOp := NewKvOperator(tr)
      idKey := kvpair.SecondClassObjectMaxKey{MaxTag:kvpair.MAXFILEIDTag, Dbid: L.destDb}
      idOp := utils.NewMaxIdOperator(tr, &idKey)
      _, err = idOp.GetCurrent()
      if err != nil {
        return nil, err
      }

      files := data.([]*sdb.LakeFileDetail)
      // modifyFiles := make([]*sdb.LakeFileDetail, 0)
      for _, file := range files {
        if L.isSameOrginaztion {
          if !file.IsShared {
            // file.IsShared = true
            // modifyFiles = append(modifyFiles, file)
          }
        }  
        fileid, err := idOp.GetLocalNext()

        if err != nil {
          return nil, err
        }

        var key kvpair.FileKey
        key.Database = L.sourceDb
        key.Relation = L.sourceRel 
        key.Fileid = fileid 

        fileNew := proto.Clone(file).(*sdb.LakeFileDetail)
    
        fileNew.BaseInfo.FileName = fmt.Sprintf("%d-%d.parquet", L.sourceDb, fileid)
        fileNew.BaseInfo.Fileid = fileid
				fileNew.Dbid = uint64(L.sourceDb)
				fileNew.Rel = uint64(L.sourceRel)
				fileNew.Xmin = uint64(1)
				fileNew.Xmax = uint64(InvaildTranscaton)
				fileNew.XminState = uint32(XS_COMMIT)
				fileNew.XmaxState = uint32(XS_NULL)
        fileNew.IsShared = true

        log.Println("insert file:", fileNew)
				err = kvOp.WritePB(&key, fileNew)
				if err != nil {
					return nil, err
				}

        copys[fileNew.BaseInfo.FileName] = file.BaseInfo.FileName
      }
    return nil, nil
  }, 3)

  //const defaultRegion = "us-east-1"
  staticResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
    return aws.Endpoint{
      PartitionID:       "aws",
      URL:               L.sourceSpace.Detail.Endpoint,//"http://localhost:9123", // or where ever you ran minio
      SigningRegion:     L.sourceSpace.Detail.Region,
      HostnameImmutable: true,
    }, nil
  })

  cfg := aws.Config{
    Region:           L.sourceSpace.Detail.Region,
    Credentials:      credentials.NewStaticCredentialsProvider(L.sourceSpace.Detail.User, L.sourceSpace.Detail.Password, ""),
    EndpointResolver: staticResolver,
  }

  s3Client := s3.NewFromConfig(cfg)
  for dest, source := range copys {
    _, err := s3Client.CopyObject(context.TODO(), &s3.CopyObjectInput{
      Bucket:     aws.String(L.destSpace.Base.Bucket),
      CopySource: aws.String(fmt.Sprintf("%v/%v", L.sourceSpace.Base.Bucket, source)),
      Key:        aws.String(fmt.Sprintf("%v", dest)),
      })
    if err != nil {
        log.Printf("Unable to copy item from bucket %q to bucket %q, %v", L.sourceSpace.Base.Bucket, L.sourceSpace.Base.Bucket, err)
    }
  }

  return nil
}
