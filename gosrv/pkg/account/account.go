package account

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	"github.com/htner/sdb/gosrv/pkg/utils"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

var ErrorPasswdMismatch error = errors.New("passwd mismatch")

type AccountMgr struct {
}

func getOne(rt fdb.ReadTransactor, key fdb.Key) ([]byte, error) {
	log.Printf("getOne called with: %T\n", rt)
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

func CheckPasswd(id uint64, passwd, enPasswd string) error {
	checkEnPasswd := fmt.Sprintf("%d-%s", id, passwd)
	sha1pwd := SHA1(checkEnPasswd)
	log.Printf("check passwd: %d %s %s %s %s", id, passwd, checkEnPasswd, sha1pwd, enPasswd)
	if sha1pwd == enPasswd {
		return nil
	}
	//return errors.New("password mismatch")
	return ErrorPasswdMismatch
}

func GetEnPasswd(id uint64, passwd string) string {
	checkEnPasswd := fmt.Sprintf("%d-%s", id, passwd)
	pwd := SHA1(checkEnPasswd)
	log.Printf("get passwd: %d %s %s %s", id, passwd, checkEnPasswd, pwd)
	return pwd
}

func GetSdbAccount(account, passwd string) (*sdb.SdbAccount, error) {
	if account == "" {
		return nil, errors.New("account must be set")
	}
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	sdbAccount, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		kvReader := fdbkv.NewKvReader(rtr)

		key := &keys.SDBAccountLoginNameKey{LoginName: account}
		idPointer := new(sdb.IdPointer)
		err := kvReader.ReadPB(key, idPointer)
		if err != nil {
			return nil, err
		}

		keySdbAccount := &keys.SDBAccountKey{Id: idPointer.Id}
		sdbAccount := new(sdb.SdbAccount)
		err = kvReader.ReadPB(keySdbAccount, sdbAccount)
		if err != nil {
			return nil, err
		}

		err = CheckPasswd(idPointer.Id, passwd, sdbAccount.Passwd)
		if err == nil {
			return sdbAccount, err
		}
		return nil, err
	})
	if sdbAccount != nil {
		return sdbAccount.(*sdb.SdbAccount), e
	}
	return nil, err
}

func CreateSdbAccount(account, passwd, organizationName string) (*sdb.SdbAccount, error) {
	if organizationName == "" {
		organizationName = account
	}
	if organizationName == "" {
		return nil, errors.New("organization must be set")
	}
	if account == "" {
		return nil, errors.New("account must be set")
	}

	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	acc, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvOp := fdbkv.NewKvOperator(tr)

		idKey := keys.FirstClassObjectMaxKey{MaxTag: keys.FCAccountMaxIDTag}
		idOp := utils.NewMaxIdOperator(tr, &idKey)
		id, err := idOp.GetNext()
		if err != nil {
			return nil, err
		}

		key := &keys.SDBAccountLoginNameKey{LoginName: account}
		idPointer := &sdb.IdPointer{Id: id}

		err = kvOp.ReadPB(key, idPointer)
		if err == nil {
			log.Println("get account:", key, idPointer)
			return nil, errors.New("sdb account exist")
		} else if err != fdbkv.ErrEmptyData {
			return nil, err
		}

		err = kvOp.WritePB(key, idPointer)
		if err != nil {
			return nil, err
		}

		keySdbAccount := &keys.SDBAccountKey{Id: id}
		sdbAccount := new(sdb.SdbAccount)
		sdbAccount.Id = id
		sdbAccount.Username = account
		sdbAccount.Passwd = GetEnPasswd(id, passwd)
		err = kvOp.WritePB(keySdbAccount, sdbAccount)
		if err != nil {
			return nil, err
		}

		idKey.MaxTag = keys.FCOrganizatoinMaxIDTag
		idOpOrgan := utils.NewMaxIdOperator(tr, &idKey)
		id, err = idOpOrgan.GetNext()
		if err != nil {
			return nil, err
		}

		keyOrganName := &keys.OrganizationNameKey{Name: organizationName}
		idPointerOrgan := &sdb.IdPointer{Id: id}

		err = kvOp.ReadPB(keyOrganName, idPointerOrgan)
		if err == nil {
			return nil, errors.New("organization name exist")
		} else if err != fdbkv.ErrEmptyData {
			return nil, err
		}

		err = kvOp.WritePB(keyOrganName, idPointerOrgan)
		if err != nil {
			return nil, err
		}

		keyOrgan := &keys.OrganizationKey{Id: id}
		organ := new(sdb.Organization)
		organ.Creator = idPointer.Id
		err = kvOp.WritePB(keyOrgan, organ)
		if err != nil {
			return nil, err
		}
		return sdbAccount, nil
	})
	if e != nil {
		return nil, e
	}
	return acc.(*sdb.SdbAccount), e
}
