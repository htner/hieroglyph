package errors

import (
	"fmt"
	"log"
)

func newErrCode(code int, format string, a ...any) error {
	text := fmt.Sprintf(format, a...)
	return &MyErrorCode{code, text}
}

func LogAndReturn(errmsg string, err error) error {
	oldErrCode := ErrorCode(err)
	format := fmt.Sprintf("%s %%v", errmsg)
	newErrMsg := newErrCode(oldErrCode.Code(), format, oldErrCode.Error())
	log.Printf("%s", newErrMsg.Error())
	return newErrMsg
}

func LogAndCreate(errmsg string, code int) error {
	errCode := newErrCode(code, "%s", errmsg)
	log.Printf("%s", errCode.Error())
	return errCode
}

type MyErrorCode struct {
	code int
	s    string
}

func (e *MyErrorCode) Error() string {
	return fmt.Sprintf("%v: %v", e.s, e.code)
}

func (e *MyErrorCode) Code() int {
	return e.code
}

func ErrorCode(err error) *MyErrorCode {
	if err == nil {
		return newErrCode(UNKNOWN, "error is corruption %s", "").(*MyErrorCode)
	}

	errorCode, ok := err.(*MyErrorCode)
	if ok {
		return errorCode
	}
	return newErrCode(UNKNOWN, "error is corruption %s", "").(*MyErrorCode)
}
