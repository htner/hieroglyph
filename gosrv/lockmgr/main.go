package main

import (
	"os"
	"os/signal"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/htner/sdb/gosrv/pkg/lockmgr"
)

// rungRPC starts a gRPC greeting server on the given port
// This is blocking, so it should be run in a goroutine
// gRPC server is stopped when the context is cancelled
// gRPC server is using reflection
func runDeadLockDetection(done chan bool) error {
  for true {
    lockmgr.DeadLock(1)
    time.Sleep(10*time.Second)
  }
	return nil
}

func main() {
	fdb.MustAPIVersion(710)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	done := make(chan bool, 1)
	go runDeadLockDetection(done)
	<-c
}
