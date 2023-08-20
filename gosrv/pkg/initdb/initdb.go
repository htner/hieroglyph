package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
)

func main() {
	initdb()
}

func initdb() {
	//src := "insert ( 1242 boolin 11 10 12 1 0 0 0 f f f t f i s 1 0 16 2275 _null_ _null_ _null_ _null_ _null_ boolin _null_ _null_ _null_ _null_ )"
	file, err := os.Open("postgres.bki")
	if err != nil {
		log.Panicf("failed reading file: %s", err)
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	l := newLexer(bufio.NewReader(bytes.NewReader(data)))
	yyParse(l)
}
