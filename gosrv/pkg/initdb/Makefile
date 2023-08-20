all: boot_lex.go boot_yacc.go
	gofmt -l -s -w *.go
	go build *.go

run: all
	./boot

boot_lex.go: bootparser.l
	golex -o $@ $<

boot_yacc.go: bootparser.y
	goyacc -o $@ $<

clean:
	go clean
	rm -f boot_yacc.go boot_lex.go lex.yy.go y.go y.output *~

nuke: clean
	rm -f boot
