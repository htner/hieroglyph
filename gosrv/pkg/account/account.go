

package account

type AccountMgr struct {
}

func GetAccount(organization, account, passwd string) {
	db, err := fdb.OpenDefault()
	if err != nil {
		return err
	}
  ret, e := rt.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			r1, _ := getOne(rtr, key1)
			r2, _ := getOne(rtr.Snapshot(), key2)
			return [][]byte{r1, r2}, nil
		})
		if e != nil {
			return nil, e
		}
		return ret.([][]byte), nil
}

func CreateSdbAccount(account, passwd, organizationName string) {

}

func CreateAccount(organization uint64, account, passwd string) {
}
