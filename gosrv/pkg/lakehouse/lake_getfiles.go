package lakehouse

import (
	"crypto/md5"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/types"
	"github.com/htner/sdb/gosrv/pkg/utils/postgres"
	"github.com/htner/sdb/gosrv/proto/sdb"

	//"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// In your code, you probably have a custom data type
// for your cluster members. Just add a String function to implement
// consistent.Member interface.
type myMember uint64 

func (m myMember) String() string {
	return fmt.Sprint("%d", m)
}

// consistent package doesn't provide a default hashing function. 
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

func (L *LakeRelOperator) GetAllFile(rel types.RelId, segCount, segIndex uint64) ([]*sdb.LakeFileDetail, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	var mgr LockMgr
	var fdblock Lock

	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = ReadLock
  fdblock.Sid = L.T.Sid

  var session *kvpair.Session

  _, err = mgr.DoWithAutoLock(db, &fdblock,
    func(tr fdb.Transaction) (interface{}, error) {
      t := L.T
      session, err = t.CheckReadAble(tr)
      return nil, err
      /*
return nil, nil
*/
    }, 3)

  if err != nil {
    log.Printf("check read able error %v", err)
    return nil, err
  }

  data, err := db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
    var key kvpair.FileKey = kvpair.FileKey{Database: L.T.Database, Relation: rel, Fileid: 0}

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
  })

	if err != nil {
		return nil, err
	}

	if data == nil || session == nil {
		return nil, errors.New("data is null")
	}

	files := data.([]*sdb.LakeFileDetail)
	// check session mvcc
  files, err = L.statifiesMvcc(files, session.WriteTranscationId)
  if err != nil {
    return nil, err
  }
  if segCount == 0 {
    return files, nil
  }
  return L.statifiesHash(rel, files, segCount, segIndex);
  // return files, err
}

/*
func (L *LakeRelOperator) GetAllFileForUpdate(rel types.RelId, segCount, segIndex uint64) ([]*sdb.LakeFileDetail, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	var mgr LockMgr
	var fdblock Lock
	fdblock.Database = L.T.Database
	fdblock.Relation = rel
	fdblock.LockType = UpdateLock
	fdblock.Sid = L.T.Sid

	var session *kvpair.Session

	data, err := mgr.DoWithAutoLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			session, err = t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

      var key kvpair.FileKey = kvpair.FileKey{Database: L.T.Database, Relation: rel, Fileid: 0}

			sKey, err := kvpair.MarshalRangePerfix(&key)
			if err != nil {
				return nil, err
			}
	
			fKey := fdb.Key(sKey)
			rr := tr.GetRange(fdb.KeyRange{Begin: fKey, End: fdb.Key{0xFF}},
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
				err = kvpair.UnmarshalKey(data.Key, &key)
				if err != nil {
					return nil, err
				}
				err = proto.Unmarshal(data.Value, file)
				if err != nil {
					return nil, err
				}
        files = append(files, file)
			}

			return files, nil
		}, 3)
	if err != nil || err == nil {
		return nil, err
	}
	files := data.([]*sdb.LakeFileDetail)
	// check session mvcc
	return L.statifiesMvcc(files, session.WriteTranscationId)
}
*/

func (L *LakeRelOperator) statifiesMvcc(files []*sdb.LakeFileDetail, currTid types.TransactionId) ([]*sdb.LakeFileDetail, error) {
  return files, nil

	statifiesFiles := make([]*sdb.LakeFileDetail, 0)
  db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
  _, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
    for _, file := range files {
      if uint64(currTid) == file.Xmax {
        continue
      }

      xminState := file.XminState
      xmaxState := file.XmaxState
      kvReader := fdbkv.NewKvReader(rtr)
      // TODO 性能优化
      {
        state, err := L.T.State(kvReader, types.TransactionId(file.Xmin))
        if err != nil {
          return nil, err
        }
        xminState = uint32(state)
      }

      if file.Xmax != uint64(InvaildTranscaton) {
        state, err := L.T.State(kvReader, types.TransactionId(file.Xmax))
        if err != nil {
          return nil, errors.New("read error")
        }
        xmaxState = uint32(state)
      }

      if xminState == uint32(XS_COMMIT) && xmaxState != uint32(XS_COMMIT) {
        statifiesFiles = append(statifiesFiles, file)
        continue
      }
      if uint64(currTid) == file.Xmin {
        statifiesFiles = append(statifiesFiles, file)
        continue
      }
    }
    return nil, nil
  })
  if e != nil {
    log.Printf("error %s", e.Error())
    return nil, e
  }
  return statifiesFiles, nil
}

func (L *LakeRelOperator) statifiesHash(rel types.RelId, files []*sdb.LakeFileDetail, segCount, segIndex uint64) ([]*sdb.LakeFileDetail, error) {
	_, ok := postgres.CatalogNames[uint32(rel)]
  if ok {
    return files, nil
  }
	statifiesFiles := make([]*sdb.LakeFileDetail, 0)

  // Create a new consistent instance
	cfg := consistent.Config{
		PartitionCount:    int(segCount),
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c := consistent.New(nil, cfg)

  for i := uint64(1); i <= segCount; i++ {
    c.Add(myMember(i))
  }
  curSegIndex := fmt.Sprint("%d", segIndex)

  for _, file := range files {
    owner := c.LocateKey([]byte(file.BaseInfo.FileName))
    if owner.String() == curSegIndex {
      statifiesFiles = append(statifiesFiles, file)
    }
  }
  return statifiesFiles, nil
}

func (L *LakeRelOperator) GetRelLakeList(rel types.RelId) (*sdb.RelLakeList, error) {
  files, err := L.GetAllFile(rel, 0, 0)
  if err != nil {
    return nil, err
  }


  md5h := md5.New()
  sha1h := sha1.New()

  relLakeList := new(sdb.RelLakeList)
  relLakeList.Rel = uint64(rel)
  
  var name string
  for _, file := range files {
    relLakeList.Files = append(relLakeList.Files, file.BaseInfo)
    name += file.BaseInfo.FileName
    io.WriteString(md5h, file.BaseInfo.FileName)
    io.WriteString(sha1h, file.BaseInfo.FileName)
  }
  relLakeList.Version = string(md5h.Sum(nil)) + string(sha1h.Sum(nil))
  return relLakeList, nil
}
