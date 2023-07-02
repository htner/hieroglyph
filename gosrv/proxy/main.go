package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
)

var options struct {
	listenAddress string
	remoteAddress string
}

func main() {

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:  %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.StringVar(&options.listenAddress, "listen", "127.0.0.1:5432", "Proxy listen address")
	flag.StringVar(&options.remoteAddress, "remote", "127.0.0.1:10002", "Remote schedule server address")
	flag.Parse()

	ln, err := net.Listen("tcp", options.listenAddress)
	if err != nil {
		log.Fatal(err)
	}

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			proxy := NewProxy(clientConn)
			err = proxy.Run()
			if err != nil {
				log.Fatal(err)
			}
		}()
	}
	<-c
}
