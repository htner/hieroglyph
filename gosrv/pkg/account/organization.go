package account

import (
	"errors"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/kvpair"
	"github.com/htner/sdb/gosrv/pkg/utils"
	"github.com/htner/sdb/gosrv/proto/sdb"
)


type OrganizationMgr struct {
}

func GetDatabase(organization, database string) (*sdb.Database, error) {
  if (organization == "") {
    return nil, errors.New("organization must be set")
  }
  if (database == "") {
    return nil, errors.New("database must be set")
  }
  db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

  sdbDB:= new(sdb.Database)
  _, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
    kvReader := fdbkv.NewKvReader(rtr)

    key := &kvpair.OrganizationNameKey{Name: organization}
    idPointer := new(sdb.IdPointer)
    err := kvReader.ReadPB(key, idPointer)
    if err != nil {
      log.Printf("read organization error %s", err.Error())
      return nil, err
    }

    keyDatabaseName:= &kvpair.DatabaseNameKey{OrganizationId:idPointer.Id, DatabaseName: database}
    err = kvReader.ReadPB(keyDatabaseName, idPointer)
    if err != nil {
      log.Printf("read database name error %s.%s->%s", organization, database, err.Error())
      return nil, err
    }

    keyDatabase := &kvpair.DatabaseKey{Id: idPointer.Id}
    err = kvReader.ReadPB(keyDatabase, sdbDB)
    if err != nil {
      log.Printf("read database error %s", err.Error())
      return nil, err
    }
    return sdbDB, err
  })
  if e != nil {
    return nil, e 
  }
  return sdbDB, e 
}

func CreateDatabase(organization, dbname string) error {
  if organization == "" {
    return errors.New("organization must be set")
  }
  if dbname == "" {
    return errors.New("database must be set")
  }
  db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}

  organizationId, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
    kvReader := fdbkv.NewKvReader(rtr)

    key := &kvpair.OrganizationNameKey{Name: organization}
    idPointer := new(sdb.IdPointer)
    err := kvReader.ReadPB(key, idPointer)
    if err != nil {
      return nil, err
    }
    return idPointer.Id, nil
  })

  if e != nil {
    return e
  }
  log.Printf("get organizationId : %d", organizationId.(uint64))

  _, e = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    kvOp := fdbkv.NewKvOperator(tr)

    idKey := kvpair.FirstClassObjectMaxKey{MaxTag:kvpair.FCDatabaseMaxIDTag}
    idOp := utils.NewMaxIdOperator(tr, &idKey)
    id, err := idOp.GetNext()
    if err != nil {
      return nil, err
    }

    key := &kvpair.DatabaseNameKey{OrganizationId: organizationId.(uint64), DatabaseName: dbname}
    idPointer := &sdb.IdPointer{}
    err = kvOp.ReadPB(key, idPointer)
    if err == nil {
      return nil, errors.New("use exist") 
    } else if (err != fdbkv.EmptyDataErr) {
      return nil, err
    }

    keyDatabase:= &kvpair.DatabaseKey{Id:id}
    sdbDatabase := new(sdb.Database)
    sdbDatabase.Dbid = id
    sdbDatabase.Dbname = dbname 
    sdbDatabase.OrganizationId = organizationId.(uint64)
    err = kvOp.WritePB(keyDatabase, sdbDatabase)
    if err != nil {
      return nil, err
    }

    idPointer.Id = id
    err = kvOp.WritePB(key, idPointer)
    if err != nil {
      return nil, err
    }
    return nil, err
  })
  return e 
}

func GetUser(organization, user, passwd string) (*sdb.User, error) {
  if organization == "" {
    return nil, errors.New("organization must be set")
  }
  if user == "" {
    return nil, errors.New("username must be set")
  }
  db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
  sdbUser, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
    kvReader := fdbkv.NewKvReader(rtr)

    key := &kvpair.OrganizationNameKey{Name: organization}
    idPointer := new(sdb.IdPointer)
    err := kvReader.ReadPB(key, idPointer)
    if err != nil {
      return nil, err
    }
    log.Printf("get organizationId : %d", idPointer.Id)

    /*
    keyOrga:= &kvpair.OrganizationKey{Id:idPointer.Id}
    organization := new(sdb.Organization)
    err = kvReader.ReadPB(keyOrga, organization)
    if err != nil {
      return nil, err
    }
    */

    log.Printf("get user %s in organizationId : %d", user, idPointer.Id)
    keyUserName := &kvpair.UserLoginNameKey{OrganizationId:idPointer.Id, LoginName: user}
    err = kvReader.ReadPB(keyUserName, idPointer)
    if err != nil {
      return nil, err
    }

    log.Printf("get user id: %d", idPointer.Id)

    keyUser:= &kvpair.UserKey{Id:idPointer.Id}
    sdbUser:= new(sdb.User)
    err = kvReader.ReadPB(keyUser, sdbUser)
    if err != nil {
      return nil, err
    }

    log.Println("get user ", sdbUser)

    err = CheckPasswd(idPointer.Id, passwd, sdbUser.Passwd)
    if err != nil {
      return nil, err 
    }
    return sdbUser, err
  })
  if sdbUser != nil {
    return sdbUser.(*sdb.User), e 
  }
  return nil, e
}

func CreateUser(organization, account, passwd string) error {
  if (organization == "") {
    return errors.New("organization must be set")
  }
  if (account == "") {
    return errors.New("username must be set")
  }
  db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
  _, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
    kvOp := fdbkv.NewKvOperator(tr)

    keyOrganName := &kvpair.OrganizationNameKey{Name: organization}
    idPointerOrgan := &sdb.IdPointer{}
    err = kvOp.ReadPB(keyOrganName, idPointerOrgan)
    if err != nil {
      return nil, err
    }

    idKey := kvpair.FirstClassObjectMaxKey{MaxTag:kvpair.FCUserMaxIDTag}
    idOp := utils.NewMaxIdOperator(tr, &idKey)
    id, err := idOp.GetNext()
    if err != nil {
      return nil, err
    }

    key := &kvpair.UserLoginNameKey{OrganizationId: idPointerOrgan.Id, LoginName: account}
    idPointer := &sdb.IdPointer{}
    err = kvOp.ReadPB(key, idPointer)
    if err == nil {
      return nil, errors.New("use exist") 
    } else if err != fdbkv.EmptyDataErr {
      return nil, err
    }

    keyUser:= &kvpair.UserKey{Id:id}
    user := new(sdb.User)
    user.Id = id
    user.OrganizationId = idPointerOrgan.Id
    user.Name = account
    user.Passwd = GetEnPasswd(id, passwd)
    err = kvOp.WritePB(keyUser, user)
    if err != nil {
      return nil, err
    }

    idPointer.Id = id
    err = kvOp.WritePB(key, idPointer)
    if err != nil {
      return nil, err
    }

    return nil, err
  })
  return e 
}
