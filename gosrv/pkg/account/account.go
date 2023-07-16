package account

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/utils"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

type AccountMgr struct {
}

func getOne(rt fdb.ReadTransactor, key fdb.Key) ([]byte, error) {
		fmt.Printf("getOne called with: %T\n", rt)
		ret, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.Get(key).MustGet(), nil
		})
		if e != nil {
			return nil, e
		}
		return ret.([]byte), nil
}

func SHA1(s string) string {
	o := sha1.New()
	o.Write([]byte(s))
	return hex.EncodeToString(o.Sum(nil))
}

func CheckPasswd(id uint64, enPasswd, passwd string) error {
  checkEnPasswd := fmt.Sprintf("%d-%s", id, passwd)
  checkEnPasswd = SHA1(checkEnPasswd)
  if (checkEnPasswd == enPasswd) {
    return nil
  }
  return errors.New("password mismatch")
}

func GetEnPasswd(id uint64, passwd string) string {
  checkEnPasswd := fmt.Sprintf("%d-%s", id, passwd)
  return SHA1(checkEnPasswd)
}


func GetSdbAccount(account, passwd string) (*sdb.SdbAccount, error) {
  if (account == "") {
    return nil, errors.New("account must be set")
  }
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
  sdbAccount, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
    kvReader := fdbkv.NewKvReader(rtr)

    key := &kvpair.SDBAccountLoginNameKey{LoginName: account}
    idPointer := new(sdb.IdPointer)
    err := kvReader.ReadPB(key, idPointer)
    if err != nil {
      return nil, err
    }

    keySdbAccount := &kvpair.SDBAccountKey{Id:idPointer.Id}
    sdbAccount:= new(sdb.SdbAccount)
    err = kvReader.ReadPB(keySdbAccount, sdbAccount)
    if err != nil {
      return nil, err
    }

    err = CheckPasswd(idPointer.Id, passwd, sdbAccount.Passwd)
    return sdbAccount, err
  })
  return sdbAccount.(*sdb.SdbAccount), e 
}

func CreateSdbAccount(account, passwd, organizationName string) error {
  if (organizationName == "") {
    organizationName = account
  }
  if (organizationName == "") {
    return errors.New("organization must be set")
  }
  if (account == "") {
    return errors.New("account must be set")
  }

  db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
  _, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    kvOp := fdbkv.NewKvOperator(tr)

    idKey := kvpair.FirstClassObjectMaxKey{MaxTag:kvpair.FCAccountMaxIDTag}
    idOp := utils.NewMaxIdOperator(tr, &idKey)
    id, err := idOp.GetNext()
    if err != nil {
      return nil, err
    }

    key := &kvpair.SDBAccountLoginNameKey{LoginName: account}
    idPointer := &sdb.IdPointer{Id:id}

    err = kvOp.ReadPB(key, idPointer)
    if err == nil {
      log.Println("get account:", key, idPointer)
      return nil, errors.New("sdb account exist") 
    } else if (err != fdbkv.EmptyDataErr) {
      return nil, err
    }


    err = kvOp.WritePB(key, idPointer)
    if err != nil {
      return nil, err
    }

    keySdbAccount := &kvpair.SDBAccountKey{Id:id}
    sdbAccount:= new(sdb.SdbAccount)
    sdbAccount.Id = id
    sdbAccount.Username = account
    sdbAccount.Passwd = GetEnPasswd(id, passwd)
    err = kvOp.WritePB(keySdbAccount, sdbAccount)
    if err != nil {
      return nil, err
    }

    idKey.MaxTag = kvpair.FCOrganizatoinMaxIDTag
    idOpOrgan := utils.NewMaxIdOperator(tr, &idKey)
    id, err = idOpOrgan.GetNext()
    if err != nil {
      return nil, err
    }

    keyOrganName := &kvpair.OrganizationNameKey{Name: organizationName}
    idPointerOrgan := &sdb.IdPointer{Id:id}

    err = kvOp.ReadPB(keyOrganName, idPointerOrgan)
    if err == nil {
      return nil, errors.New("organization name exist") 
    } else if (err != fdbkv.EmptyDataErr) {
      return nil, err
    }


    err = kvOp.WritePB(keyOrganName, idPointerOrgan)
    if err != nil {
      return nil, err
    }

    keyOrgan := &kvpair.OrganizationKey{Id:id}
    organ := new(sdb.Organization)
    organ.Creator = idPointer.Id
    err = kvOp.WritePB(keyOrgan, organ)
    if err != nil {
      return nil, err
    }
    return nil, err
  })
  return e 
}


