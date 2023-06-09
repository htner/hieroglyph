package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
)

var options struct {
	listenAddress string
	remoteAddress string
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:  %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.StringVar(&options.listenAddress, "listen", "127.0.0.1:5432", "Proxy listen address")
	flag.StringVar(&options.remoteAddress, "remote", "127.0.0.1:15432", "Remote schedule server address")
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

		proxy := NewProxy(clientConn)
		err = proxy.Run()
		if err != nil {
			log.Fatal(err)
		}
	}
}
