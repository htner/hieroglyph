package lockmgr

import (
	/*
		"bytes"
		"crypto/md5"
		"crypto/sha1"
		"errors"
		"fmt"
		"io"
		"math"
	*/

	"github.com/htner/sdb/gosrv/proto/sdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/heimdalr/dag"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/schedule"
	_ "github.com/htner/sdb/gosrv/pkg/utils/logformat"
	log "github.com/sirupsen/logrus"
)

func DeadLock(dbid uint64) error {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}

  locks := make([]*keys.Lock, 0)
  plocks := make([]*keys.PotentialLock, 0)

	_, err = db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
    key := keys.Lock{Database: dbid, Relation: 0}

		sKeyStart, err := fdbkv.MarshalRangePerfix(&key)
		if err != nil {
			log.Printf("marshal ranage perfix %v", err)
			return nil, err
		}
    rangekey, err := fdb.PrefixRange(sKeyStart)
		rr := tr.GetRange(rangekey, fdb.RangeOptions{})
		ri := rr.Iterator()

		for ri.Advance() {
			data, e := ri.Get()
			if e != nil {
				log.Printf("Unable to read next value: %v\n", e)
				return nil, nil
			}
			var lock keys.Lock
			err = fdbkv.UnmarshalKey(data.Key, &lock)
			if err != nil {
				log.Printf("UnmarshalKey error ? %v %v", data, err)
				return nil, err
			}
      log.Println("lock ", lock)
			locks = append(locks, &lock)
		}

    pkey := keys.PotentialLock{Database: dbid, Relation: 0}

		sKeyStart, err = fdbkv.MarshalRangePerfix(&pkey)
		if err != nil {
			log.Printf("marshal ranage perfix %v", err)
			return nil, err
		}
    rangekey, err = fdb.PrefixRange(sKeyStart)
		rr = tr.GetRange(rangekey, fdb.RangeOptions{})
		ri = rr.Iterator()

		for ri.Advance() {
			data, e := ri.Get()
			if e != nil {
				log.Printf("Unable to read next value: %v\n", e)
				return nil, nil
			}
			var lock keys.PotentialLock
			err = fdbkv.UnmarshalKey(data.Key, &lock)
			if err != nil {
				log.Printf("UnmarshalKey error ? %v %v", data, err)
				return nil, err
			}
			plocks = append(plocks, &lock)
      log.Println("plock ", lock)
		}
	
		return nil, nil
	})

	if err != nil {
		return err
	}

  d := dag.NewDAG()

  ids := make(map[uint64]string)
  curs := make(map[string]uint64)
  for _, lock := range locks {
    var fakeLock keys.Lock//proto.Clone(lock)
    fakeLock = *lock
    fakeLock.Sid = 0
    data, _ := fdbkv.MarshalKey(&fakeLock)
    curs[string(data)] = lock.Sid 

    _, ok := ids[lock.Sid]
    if !ok {
      id, err := d.AddVertex(lock.Sid) 
      log.Println("lock add vertex:", lock.Sid, id, err)
      if err == nil {
        ids[lock.Sid] = id
      }
    }
  }

  for _, plock := range plocks {
    _, ok := ids[plock.Sid]
    if !ok {
      id, err := d.AddVertex(plock.Sid) 
      log.Println("plock add vertex:", plock.Sid, id, err)
      if err != nil {
        ids[plock.Sid] = id
      }
    }
  }

  for _, plock := range plocks {
    var lock keys.Lock
    lock.Database = plock.Database
    lock.Relation = plock.Relation
    lock.LockType = plock.LockType
    lock.Sid = 0  
    data, _ := fdbkv.MarshalKey(&lock)
    toSid, ok := curs[string(data)]
    if !ok {
      log.Printf("string data :", string(data))
    } else {
      toVid, ok := ids[toSid]
      if !ok {
        log.Printf("to sid:", toSid)
      }
      fromVid, ok := ids[plock.Sid]
      if !ok {
        log.Printf("from sid:", plock.Sid)
      }
      err := d.AddEdge(fromVid, toVid)
      log.Println("add edge error:", fromVid, plock.Sid, toVid, toSid, err)

      _, ok = err.(dag.EdgeLoopError)
      if ok {
        tr := lakehouse.NewTranscation(dbid, plock.Sid)
        curSession := tr.GetSession()
        if curSession.QueryId != 0 { 
          mgr := schedule.NewQueryMgr(dbid)
          mgr.InitQueryResult(curSession.QueryId, uint32(sdb.QueryStates_QueryError), 1, "dead lock")
        }
        tr.Rollback()
      }
    }
  }
  return nil
}
