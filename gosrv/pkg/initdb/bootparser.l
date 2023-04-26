%{
// Copyright (c) 2011 CZ.NIC z.s.p.o. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"bufio"
	"modernc.org/golex/lex"
    "log"
)

type lexer struct {
	*lex.Lexer
}

type yylexer struct{
    src     *bufio.Reader
    buf     []byte
    empty   bool
    current byte
	prev byte
}

func newLexer(src *bufio.Reader) (y *yylexer) {
    y = &yylexer{src: src}
	y.buf = make([]byte, 0)
	y.empty = false
	y.prev = 0
    if b, err := src.ReadByte(); err == nil {
        y.current = b
    }
    return
}

func (y *yylexer) getc() byte {
	y.prev = y.current
    if y.current != 0 {
        y.buf = append(y.buf, y.current)
    }
    y.current = 0
    if b, err := y.src.ReadByte(); err == nil {
        y.current = b
    }
    return y.current
}

func (y yylexer) Error(e string) {
    log.Fatal(e)
}

func (y *yylexer) Lex(lval *yySymType) int {
	//log.Println("get next sym")
    c := y.current
    if y.empty {
        c, y.empty = y.getc(), false
    }
%}

%yyt 0
%yyb (y.prev == 0 || y.prev == '\n')
%yyc c
%yyn c = y.getc()

D		[0-9]
oct		\\{D}{D}{D}
Exp		[Ee][-+]?{D}+
id		([A-Za-z0-9_]|{oct}|\-)+
sid		\"([^\"])*\" 
sid2     \'([^\'])*\'
arrayid [A-Za-z0-9_]+\[{D}*\]

%%
	y.buf = y.buf[:0]


<*>\0	return 0


^#[^\n]*	{
					 log.Println("pass comments", string(y.buf), string(y.current));
				}

[ \t\n\r]+  |
\/\/.*      |
\/\*([^*]|\*+[^*/])*\*+\/

open			lval.str = string(y.buf); return OPEN

close			lval.str = string(y.buf); return XCLOSE

create		{
	log.Println("create something")
	lval.str = string(y.buf); return XCREATE
	}

OID				lval.str = string(y.buf); return OBJ_ID
bootstrap		lval.str = string(y.buf); return XBOOTSTRAP
"shared_relation"	lval.str = string(y.buf); return XSHARED_RELATION
"without_oids"	lval.str = string(y.buf); return XWITHOUT_OIDS
"rowtype_oid"	lval.str = string(y.buf); return XROWTYPE_OID
_null_			lval.str = string(y.buf); return NULLVAL

insert			lval.str = string(y.buf); return INSERT_TUPLE

","				return COMMA
"="				return EQUALS
"("				return LPAREN
")"				return RPAREN

"declare"		lval.str = string(y.buf); return XDECLARE
"build"			lval.str = string(y.buf); return XBUILD
"indices"		lval.str = string(y.buf); return INDICES
"unique"		lval.str = string(y.buf); return UNIQUE
"index"			lval.str = string(y.buf); return INDEX
"on"			lval.str = string(y.buf); return ON
"using"			lval.str = string(y.buf); return USING
"toast"			lval.str = string(y.buf); return XTOAST
"FORCE"			lval.str = string(y.buf); return XFORCE 
"NOT"			lval.str = string(y.buf); return XNOT 
"NULL"			lval.str = string(y.buf); return XNULL

{arrayid}		{
					lval.str = string(y.buf); return ID
				}
{id}			{
                    lval.str = string(y.buf)
					//log.Println("id", string(y.buf));
                    return ID
				}
{sid}			{
					lval.str = string(y.buf)
					return ID
				}

{sid2}			{
					lval.str = string(y.buf)
					return ID
				}

(-)?{D}+"."{D}*({Exp})? |
(-)?{D}*"."{D}+({Exp})? |
(-)?{D}+{Exp}			{
							lval.str = string(y.buf)
							log.Println("const", string(y.buf));
							return CONST_P
						}

.				{
					 log.Println("syntax error?", string(y.buf), string(y.current));
					 fmt.Printf("%q\n", y.buf)
				}

%%
	{
					 log.Println("end syntax error?", string(y.buf), string(y.current));
	}
	log.Fatal("scanner internal error")
 	y.empty = true
    return NOTHING
}

	
