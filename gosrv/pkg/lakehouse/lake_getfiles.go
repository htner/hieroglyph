package lakehouse

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	"github.com/htner/sdb/gosrv/pkg/utils/postgres"
	"github.com/htner/sdb/gosrv/proto/sdb"
	log "github.com/sirupsen/logrus"

	//"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// In your code, you probably have a custom data type
// for your cluster members. Just add a String function to implement
// consistent.Member interface.
type myMember struct {
	hash string
	id   uint64
}

func buildMyMemeber(id uint64) myMember {

	return myMember{hash: fmt.Sprintf("%d", id), id: id}
}

func (m myMember) String() string {
	return m.hash
}

// consistent package doesn't provide a default hashing function.
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

func (L *LakeRelOperator) GetAllFile(rel uint64, segCount, segIndex uint64) ([]*sdb.LakeFileDetail, error) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

  if !L.isSuper {
    _, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
      t := L.T
      err = t.CheckReadAble(tr)
      return nil, err
    })

    if err != nil {
      log.Printf("check read able error %v", err)
      return nil, err
    }
  }

	data, err := db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		var key keys.FileKey = keys.FileKey{Database: L.T.Database, Relation: rel, Fileid: 0}

		sKeyStart, err := fdbkv.MarshalRangePerfix(&key)
		if err != nil {
			log.Printf("marshal ranage perfix %v", err)
			return nil, err
		}
		key.Fileid = math.MaxUint64
		sKeyEnd, err := fdbkv.MarshalRangePerfix(&key)
		if err != nil {
			log.Printf("marshal ranage perfix %v", err)
			return nil, err
		}

		keyStart := fdb.Key(sKeyStart)
		keyEnd := fdb.Key(sKeyEnd)
		rr := tr.GetRange(fdb.KeyRange{Begin: keyStart, End: keyEnd},
			fdb.RangeOptions{Limit: 10000})
		ri := rr.Iterator()

		// Advance will return true until the iterator is exhausted
		files := make([]*sdb.LakeFileDetail, 0)
		for ri.Advance() {
			file := &sdb.LakeFileDetail{}
			data, e := ri.Get()
			// log.Println(data)
			if e != nil {
				log.Printf("Unable to read next value: %v\n", e)
				return nil, nil
			}
			var key keys.FileKey
			err = fdbkv.UnmarshalKey(data.Key, &key)
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

	if data == nil {
		return nil, errors.New("data is null")
	}

	files := data.([]*sdb.LakeFileDetail)
	// check session mvcc
  if !L.isSuper {
    files, err = L.statifiesMvcc(files, L.T.session.WriteTransactionId)
  } else {
    files, err = L.statifiesMvcc(files, SUPER_SESSION)
  }
	if err != nil {
		return nil, err
	}
	if segCount == 0 {
		return files, nil
	}
	return L.statifiesHash(rel, files, segCount, segIndex)
	// return files, err
}

/*
func (L *LakeRelOperator) GetAllFileForUpdate(rel uint64, segCount, segIndex uint64) ([]*sdb.LakeFileDetail, error) {
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

	var session *keys.Session

	data, err := mgr.DoWithAutoLock(db, &fdblock,
		func(tr fdb.Transaction) (interface{}, error) {
			t := L.T
			session, err = t.CheckWriteAble(tr)
			if err != nil {
				return nil, err
			}

      var key keys.FileKey = keys.FileKey{Database: L.T.Database, Relation: rel, Fileid: 0}

			sKey, err := keys.MarshalRangePerfix(&key)
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
				err = keys.UnmarshalKey(data.Key, &key)
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

func (L *LakeRelOperator) statifiesMvcc(files []*sdb.LakeFileDetail, currTid uint64) ([]*sdb.LakeFileDetail, error) {
	// return files, nil
	statifiesFiles := make([]*sdb.LakeFileDetail, 0)
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	_, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		for _, file := range files {
			// log.Println("tid ", currTid, "file xmin:", file.Xmin)
			if file.Xmax != 0 && uint64(currTid) == file.Xmax {
				// log.Println("error tid == file.Xmax:", currTid)
				continue
			}

			xminState := file.XminState
			xmaxState := file.XmaxState
			kvReader := fdbkv.NewKvReader(rtr)
			// TODO 性能优化
			if file.Xmin == 1 {
				xminState = uint32(XS_COMMIT)
			} else {
				state, err := L.T.State(kvReader, uint64(file.Xmin))
				if err != nil {
					log.Println("not found min state:", file.Xmin, file)
					return nil, err
				}
				xminState = uint32(state)
			}
			// log.Println("xmin state:", xminState, " xmax state:", xmaxState)

			if file.Xmax != uint64(InvaildTranscaton) {
				state, err := L.T.State(kvReader, uint64(file.Xmax))
				if err != nil {
					log.Println("not found max state:", file.Xmax)
					return nil, errors.New("read error")
				}
				xmaxState = uint32(state)
			}

			// log.Println("xmin ", file.Xmin, " state:", xminState, " xmax ", file.Xmax," state:", xmaxState)

			if xminState == uint32(XS_COMMIT) && xmaxState != uint32(XS_COMMIT) {
				statifiesFiles = append(statifiesFiles, file)
				continue
			}
			if uint64(currTid) == file.Xmin {
				statifiesFiles = append(statifiesFiles, file)
				continue
			}
			log.Println("statifiesFiles error, can not append xmin:", xminState, " xmax:", xmaxState)
		}
		return statifiesFiles, nil
	})
	if e != nil {
		log.Printf("error %s", e.Error())
		return nil, e
	}
	return statifiesFiles, nil
}

func (L *LakeRelOperator) statifiesHash(rel uint64, files []*sdb.LakeFileDetail, segCount, segIndex uint64) ([]*sdb.LakeFileDetail, error) {
	_, ok := postgres.CatalogNames[rel]
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

	for i := uint64(0); i < segCount; i++ {
		c.Add(buildMyMemeber(i))
	}
	curMember := buildMyMemeber(segIndex)

	for _, file := range files {
		owner := c.LocateKey([]byte(fmt.Sprintf("%d", file.BaseInfo.FileId)))
		log.Println(owner, file)
		//if mem, ok := owner.(myMember)
		if owner.String() == curMember.String() {
			statifiesFiles = append(statifiesFiles, file)
		}
	}
	return statifiesFiles, nil
}

func (L *LakeRelOperator) GetRelLakeList(rel uint64) (*sdb.RelFiles, error) {
	files, err := L.GetAllFile(rel, 0, 0)
	if err != nil {
		return nil, err
	}

	md5h := md5.New()
	sha1h := sha1.New()

	relLakeList := new(sdb.RelFiles)
	relLakeList.Rel = uint64(rel)

	var name string
	for _, file := range files {
		relLakeList.Files = append(relLakeList.Files, file.BaseInfo)
		name += fmt.Sprintf("%d", file.BaseInfo.FileId)
		io.WriteString(md5h, fmt.Sprintf("%d", file.BaseInfo.FileId))
		io.WriteString(sha1h, fmt.Sprintf("%d", file.BaseInfo.FileId))
	}

	var buffer bytes.Buffer
	buffer.Write(md5h.Sum(nil))
	buffer.Write(sha1h.Sum(nil))
	relLakeList.Version = buffer.Bytes()
	return relLakeList, nil
}
