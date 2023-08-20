package logformat

import (
	"fmt"
	"path"
	"runtime"

	log "github.com/sirupsen/logrus"
)

func init() {
	SetLogger(true)
}

// Only the msg is printed, which may be needed for the final release
type MsgFormatter struct {
}

// Format the msg only
func (f *MsgFormatter) Format(entry *log.Entry) ([]byte, error) {
	// Note this doesn't include Time, Level and Message which are available on
	// the Entry. Consult `godoc` on information about those fields or read the
	// source of the official loggers.
	return []byte(entry.Message + "\n"), nil
}

// Set logger for debug/release
func SetLogger(debug bool) {
	if !debug {
		log.SetLevel(log.InfoLevel)
		log.SetFormatter(&log.TextFormatter{})
	} else {
		log.SetReportCaller(true)
		log.SetLevel(log.DebugLevel)
		log.SetFormatter(&log.TextFormatter{
			TimestampFormat: "2006-01-02 15:03:04",
			CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
				fileInfo := fmt.Sprintf("%s:%d", path.Base(frame.File), frame.Line)
				functionName := path.Base(frame.Function)
				return functionName, fileInfo
			},
		})
	}
}
