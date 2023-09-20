package lakehouse

import (
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/config"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	"github.com/htner/sdb/gosrv/pkg/utils"
	"github.com/htner/sdb/gosrv/pkg/utils/postgres"
	"github.com/htner/sdb/gosrv/proto/sdb"
	"google.golang.org/protobuf/proto"
	/*
		"github.com/aws/aws-sdk-go-v2/aws"
		"github.com/aws/aws-sdk-go-v2/credentials"
		"github.com/aws/aws-sdk-go-v2/service/s3"
	*/)

type LakeOperator struct {
}

func (L *LakeOperator) Copy(dest, source uint64, session uint64) {
	// only catatlog copy now, copy all in the futrue
	var conf config.LakeSpaceConfig
  space, _ := conf.GetConfig(dest)

	catalogs := postgres.CatalogNames
	for rel := range catalogs {
		relCopy := new(LakeRelCopyOperator)
		relCopy.destDb = dest
		relCopy.destRel = rel
		relCopy.destSpace = space

		relCopy.sourceDb = source
		relCopy.sourceRel = rel
		relCopy.sourceSpace = space

    err := relCopy.Copy(session)
    if err != nil {
      break
    }
	}
}

type LakeDBCopyOperator struct {
	dest   uint64
	source uint64
}

type LakeRelCopyOperator struct {
	destRel   uint64
	destDb    uint64
	destSpace *sdb.LakeSpace

	sourceRel   uint64
	sourceDb    uint64
	sourceSpace *sdb.LakeSpace

	isSameOrginaztion bool
}

func (L *LakeRelCopyOperator) Copy(session uint64) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
	var mgr LockMgr

  // lock source
  err = mgr.TryLock(L.sourceDb, session, L.sourceRel)
	if err != nil {
		return err
	}
  defer mgr.TryUnlock(L.sourceDb, session, L.sourceRel)

  lakeRel := NewLakeRelOperator(L.sourceDb, session)
	files, err := lakeRel.GetAllFile(L.sourceRel, 0, 0)
	if err != nil {
		return err
	}
  log.Println("get files", files)

  _, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    kvOp := NewKvOperator(tr)
    idKey := keys.SecondClassObjectMaxKey{MaxTag: keys.MAXFILEIDTag, Dbid: 0}
    idOp := utils.NewMaxIdOperator(tr, &idKey)
    _, err = idOp.GetCurrent()
    if err != nil {
      return nil, err
    }

    newShareds := make(map[uint64]*[]*sdb.Relation)

    for _, file := range files {
      fileid := file.BaseInfo.FileId
      if !file.IsShared {
        file.IsShared = true
        share := &sdb.Relation{Rel:file.Rel, Dbid: file.Dbid}
        newShareds[fileid] = &[]*sdb.Relation{share}

        var key keys.FileKey
        key.Database = L.sourceDb
        key.Relation = L.sourceRel
        key.Fileid = fileid
        // update IsShared
        err = kvOp.WritePB(&key, file)

        if err != nil {
          return nil, err
        }
      }

      if err != nil {
        return nil, err
      }

      var key keys.FileKey
      key.Database = L.destDb
      key.Relation = L.destRel
      key.Fileid = fileid

      fileNew := proto.Clone(file).(*sdb.LakeFileDetail)

      //fileNew.BaseInfo.FileName = fmt.Sprintf("%d-%d.parquet", L.sourceDb, fileid)
      fileNew.BaseInfo.FileId = fileid

      fileNew.Dbid = L.destDb
      fileNew.Rel = L.destRel
      fileNew.Xmin = uint64(1)
      fileNew.Xmax = uint64(InvaildTranscaton)
      fileNew.XminState = uint32(XS_COMMIT)
      fileNew.XmaxState = uint32(XS_NULL)
      fileNew.IsShared = true 

      share := &sdb.Relation{Rel:fileNew.Rel, Dbid: fileNew.Dbid}
      shareds, ok := newShareds[fileid]
      if ok {
        *shareds = append(*shareds, share)
      } else {
        newShareds[fileid] = &[]*sdb.Relation{share}
      }

      log.Println("insert file:", key, fileNew)
      err = kvOp.WritePB(&key, fileNew)
      if err != nil {

      }

      for fileid, shareds := range newShareds {
        kvOp := NewKvOperator(tr)
        idKey := keys.FileSharedKey{Fileid: fileid}
        sharedsPB := new(sdb.LakeFileShared)
        err = kvOp.ReadPB(&idKey, sharedsPB)
        if err != nil {
          if err != fdbkv.ErrEmptyData {
            return nil, err
          }
        }
        sharedsPB.SharedRels = append(sharedsPB.SharedRels, *shareds...)

        err = kvOp.WritePB(&idKey, sharedsPB)
        if err != nil {
          return nil, err
        }
      }
      //copys = append(copys, file.BaseInfo)
    }
    return nil, nil
  })

	//const defaultRegion = "us-east-1"
  /*
	staticResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               L.sourceSpace.S3Info.Endpoint, //"http://localhost:9123", // or where ever you ran minio
			SigningRegion:     L.sourceSpace.S3Info.Region,
			HostnameImmutable: true,
		}, nil
	})

	cfg := aws.Config{
		Region:           L.sourceSpace.S3Info.Region,
		Credentials:      credentials.NewStaticCredentialsProvider(L.sourceSpace.S3Info.User, L.sourceSpace.S3Info.Password, ""),
		EndpointResolver: staticResolver,
	}

	s3Client := s3.NewFromConfig(cfg)
	for index, source := range copys {
    oldfile := files[index]
    filename := fmt.Sprintf("%s/%d.parquet", L.sourceSpace.S3Info.Bucket, source.FileId)
		_, err := s3Client.CopyObject(context.TODO(), &s3.CopyObjectInput{
			Bucket:     aws.String(L.destSpace.S3Info.Bucket),
			CopySource: aws.String(filename),
			Key:        aws.String(fmt.Sprintf("%d.parquet", source.FileId)),
		})
		if err != nil {
			log.Printf("Unable to copy item from bucket %q to bucket %q, %v", L.sourceSpace.S3Info.Bucket, L.destSpace.S3Info.Bucket, err)
		}
	}
  */
  log.Println("copy rel error:", err)

	return err 
}
