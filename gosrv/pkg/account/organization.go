package account

import (
	"errors"

	log "github.com/sirupsen/logrus"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/fdbkv"
	"github.com/htner/sdb/gosrv/pkg/fdbkv/keys"
	"github.com/htner/sdb/gosrv/pkg/lakehouse"
	"github.com/htner/sdb/gosrv/pkg/utils"
	"github.com/htner/sdb/gosrv/proto/sdb"
)

type OrganizationMgr struct {
}

func GetDatabase(organization, database string) (*sdb.Database, error) {
	if organization == "" {
		return nil, errors.New("organization must be set")
	}
	if database == "" {
		return nil, errors.New("database must be set")
	}
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	sdbDB := new(sdb.Database)
	_, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		kvReader := fdbkv.NewKvReader(rtr)

		key := &keys.OrganizationNameKey{Name: organization}
		idPointer := new(sdb.IdPointer)
		err := kvReader.ReadPB(key, idPointer)
		if err != nil {
			log.Printf("read organization error %s", err.Error())
			return nil, err
		}

		keyDatabaseName := &keys.DatabaseNameKey{OrganizationId: idPointer.Id, DatabaseName: database}
		err = kvReader.ReadPB(keyDatabaseName, idPointer)
		if err != nil {
			log.Printf("read database name error %s.%s->%s", organization, database, err.Error())
			return nil, err
		}

		keyDatabase := &keys.DatabaseKey{Id: idPointer.Id}
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

func CreateDatabase(organization, dbname string) (*sdb.Database, error) {
	if organization == "" {
		return nil, errors.New("organization must be set")
	}
	if dbname == "" {
		return nil, errors.New("database must be set")
	}
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	organizationId, e := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		kvReader := fdbkv.NewKvReader(rtr)

		key := &keys.OrganizationNameKey{Name: organization}
		idPointer := new(sdb.IdPointer)
		err := kvReader.ReadPB(key, idPointer)
		if err != nil {
			return nil, err
		}
		return idPointer.Id, nil
	})

	if e != nil {
		return nil, e
	}
	log.Printf("get organizationId : %d", organizationId.(uint64))

	// dbid := uint64(0)
	sdbDatabase, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvOp := fdbkv.NewKvOperator(tr)

		idKey := keys.FirstClassObjectMaxKey{MaxTag: keys.FCDatabaseMaxIDTag}
		idOp := utils.NewMaxIdOperator(tr, &idKey)
		id, err := idOp.GetNext()
		if err != nil {
			return nil, err
		}

		key := &keys.DatabaseNameKey{OrganizationId: organizationId.(uint64), DatabaseName: dbname}
		idPointer := &sdb.IdPointer{}
		err = kvOp.ReadPB(key, idPointer)
		if err == nil {
			return nil, errors.New("use exist")
		} else if err != fdbkv.ErrEmptyData {
			return nil, err
		}

		keyDatabase := &keys.DatabaseKey{Id: id}
		sdbDatabase := new(sdb.Database)
		sdbDatabase.Dbid = id
		sdbDatabase.Dbname = dbname
		sdbDatabase.OrganizationId = organizationId.(uint64)
		err = kvOp.WritePB(keyDatabase, sdbDatabase)
		if err != nil {
			return nil, err
		}

		idPointer.Id = id
		// dbid = id
		err = kvOp.WritePB(key, idPointer)
		if err != nil {
			return nil, err
		}
		return sdbDatabase, err
	})
	if e == nil {
    /*
		sess, err := lakehouse.CreateSession(1, 1)
		if err != nil {
			return nil, err
		}
		lakeop := new(lakehouse.LakeOperator)
		tr := lakehouse.NewTranscation(sess.Dbid, sess.Id)
		// only for old table
		tr.Start(true)
		lakeop.Copy(sess.Dbid, uint64(dbid), sess.Id)
		tr.Commit()
    */
		return sdbDatabase.(*sdb.Database), e
	}
	return nil, e
}

func GetOrganizationId(db fdb.Database, organization string) (uint64, error) {
  id, err := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		kvReader := fdbkv.NewKvReader(rtr)

		key := &keys.OrganizationNameKey{Name: organization}
		idPointer := new(sdb.IdPointer)
		err := kvReader.ReadPB(key, idPointer)
		if err != nil {
			return uint64(0), err
		}
		return idPointer.Id, nil
	})
  return id.(uint64), err
}

func CloneDatabase(organization, dbname, sourceOrganization, sourceDbname string, uid uint64) (*sdb.Database, error) {
  sourceDatabase, err := GetDatabase(sourceOrganization, sourceDbname)
  if err != nil {
    return nil, err
  }
  newDatabase, err := CreateDatabase(organization, dbname)
  if err != nil {
    return nil, err 
  }

  sess, err := lakehouse.CreateSession(sourceDatabase.Dbid, uid)
  if err != nil {
    return nil, err
  }
  lakeop := new(lakehouse.LakeOperator)
  tr := lakehouse.NewTranscation(sess.Dbid, sess.Id)
  tr.Start(true)
  lakeop.Copy(newDatabase.Dbid, sess.Dbid, sess.Id)
  tr.Commit()
  return newDatabase, nil
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

		key := &keys.OrganizationNameKey{Name: organization}
		idPointer := new(sdb.IdPointer)
		err := kvReader.ReadPB(key, idPointer)
		if err != nil {
			return nil, err
		}
		log.Printf("get organizationId : %d", idPointer.Id)

		/*
		   keyOrga:= &keys.OrganizationKey{Id:idPointer.Id}
		   organization := new(sdb.Organization)
		   err = kvReader.ReadPB(keyOrga, organization)
		   if err != nil {
		     return nil, err
		   }
		*/

		log.Printf("get user %s in organizationId : %d", user, idPointer.Id)
		keyUserName := &keys.UserLoginNameKey{OrganizationId: idPointer.Id, LoginName: user}
		err = kvReader.ReadPB(keyUserName, idPointer)
		if err != nil {
			return nil, err
		}

		log.Printf("get user id: %d", idPointer.Id)

		keyUser := &keys.UserKey{Id: idPointer.Id}
		sdbUser := new(sdb.User)
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

func CreateUser(organization, account, passwd string) (*sdb.User, error) {
	if organization == "" {
		return nil, errors.New("organization must be set")
	}
	if account == "" {
		return nil, errors.New("username must be set")
	}
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}
	user, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kvOp := fdbkv.NewKvOperator(tr)

		keyOrganName := &keys.OrganizationNameKey{Name: organization}
		idPointerOrgan := &sdb.IdPointer{}
		err = kvOp.ReadPB(keyOrganName, idPointerOrgan)
		if err != nil {
			return nil, err
		}

		idKey := keys.FirstClassObjectMaxKey{MaxTag: keys.FCUserMaxIDTag}
		idOp := utils.NewMaxIdOperator(tr, &idKey)
		id, err := idOp.GetNext()
		if err != nil {
			return nil, err
		}

		key := &keys.UserLoginNameKey{OrganizationId: idPointerOrgan.Id, LoginName: account}
		idPointer := &sdb.IdPointer{}
		err = kvOp.ReadPB(key, idPointer)
		if err == nil {
			return nil, errors.New("use exist")
		} else if err != fdbkv.ErrEmptyData {
			return nil, err
		}

		keyUser := &keys.UserKey{Id: id}
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

		return user, err
	})
	if e != nil {
		return nil, e
	}
	return user.(*sdb.User), e
}
