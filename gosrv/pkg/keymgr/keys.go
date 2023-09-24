package keymgr

const LastVersion uint64 = 0

func IsTriSecretMode() {
}

// AES 256-bit encryption
func GetTheRootKey(account uint64, version uint16) (string, uint64, error) {
	// why account not organization?
	// func GetOrganizationKey(ori uint64, version uint64) (string, uint64, error) {
	return "123", 1, nil
}

func GetCustomerKey(ori uint64, version uint32) (string, uint64, error) {

}

// AES 256-bit encryption
func GetAccountKey(ori uint64, version uint32) (string, uint64, error) {
	// why account not organization?
	// func GetOrganizationKey(ori uint64, version uint64) (string, uint64, error) {
	//if ()
	return "123", 1, nil
}

func GetDatabaseKey(database uint64, version uint64) (string, uint64, error) {
	return "1234", 1, nil
}

func GetTableKey(database uint64, relation uint64, version uint64) (string, uint64, error) {
	return "12345", 1, nil
}

func EFileKey(database uint64, relation uint64, fileid uint64, filekey string) (string, uint64, error) {
	return "12345", 1, nil
}

func DFileKey(database uint64, relation uint64, fileid uint64, efilekey string) (string, uint64, error) {
	return "12345", 1, nil
}
