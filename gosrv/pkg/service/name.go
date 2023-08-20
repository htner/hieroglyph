package service

import (
	"strings"

	"github.com/htner/sdb/gosrv/proto/sdb"
)

func ServiceName(name string) string {
	return strings.ReplaceAll(name, "sdb.", "sdb_")
}

func ScheduleName() string {
	return ServiceName(sdb.Schedule_ServiceDesc.ServiceName)
}

func LakeName() string {
	return ServiceName(sdb.Lake_ServiceDesc.ServiceName)
}

func ProxyName() string {
	return "sdb_proxy"
}

func AccountName() string {
	return ServiceName(sdb.Account_ServiceDesc.ServiceName)
}
