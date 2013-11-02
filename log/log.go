// gist.github.com/alphazero/2718939
// a very thin convenience wrapper over go's log package
// that also alters the semantics of go log's Error and Fatal:
// none of the functions of this package will ever panic() or
// call os.Exit().
//
// Log levels are prefixed to the user log data for each
// eponymous function i.e. logger.Error(..) will emit a log
// message that begins (after prefix) with ERROR - ..
//
// Package also opts for logger.Lmicroseconds prefix in addition
// to the default Prefix().
package log

import (
	"fmt"
	"io"
	stdlib "log"
	"os"
	"time"
)

const (
	delim   = " - "
	FATAL   = "[FATAL]"
	ERROR   = "[ERROR]"
	WARN    = "[WARN ]"
	DEBUG   = "[DEBUG]"
	INFO    = "[INFO ]"
	levlen  = len(INFO)
	len2    = 2*len(delim) + levlen
	len3    = 3*len(delim) + levlen
	tFormat = "2006-01-02 15:04:05.999999999 -0700 MST"
)

var levels = []string{FATAL, ERROR, WARN, DEBUG, INFO}

type logger struct {
	logger *stdlib.Logger
}

var l = New(os.Stdout, "", 0)

func New(out io.Writer, prefix string, flag int) *logger {
	return &logger{stdlib.New(out, prefix, flag)}
}

// NOTE - the semantics here are different from go's logger.Fatal
// It will neither panic nor exit
func Fatal(meta string, e error) {
	l.logger.Println(join3(FATAL, meta, e.Error()))
}

// NOTE - the semantics here are different from go's logger.Fatal
// It will neither panic nor exit
func Fatalf(format string, v ...interface{}) {
	l.logger.Println(join2(FATAL, fmt.Sprintf(format, v...)))
}

func Error(meta string, e error) {
	l.logger.Println(join3(ERROR, meta, e.Error()))
}
func Errorf(format string, v ...interface{}) {
	l.logger.Println(join2(ERROR, fmt.Sprintf(format, v...)))
}

func Debug(msg string) {
	l.logger.Println(join2(DEBUG, msg))
}

func Debugf(format string, v ...interface{}) {
	l.logger.Println(join2(DEBUG, fmt.Sprintf(format, v...)))
}

func Warn(msg string) {
	l.logger.Println(join2(WARN, msg))
}

func Warnf(format string, v ...interface{}) {
	l.logger.Println(join2(WARN, fmt.Sprintf(format, v...)))
}

func Info(msg string) {
	l.logger.Println(join2(INFO, msg))
}

func Infof(format string, v ...interface{}) {
	l.logger.Println(join2(INFO, fmt.Sprintf(format, v...)))
}

func join2(level, msg string) string {
	t := time.Now().Round(time.Microsecond).Format(tFormat)
	n := len(msg) + len2 + len(t)
	j := make([]byte, n)
	o := copy(j, level)
	o += copy(j[o:], delim)
	o += copy(j[o:], t)
	o += copy(j[o:], delim)
	copy(j[o:], msg)
	return string(j)
}
func join3(level, meta, msg string) string {
	t := time.Now().Round(time.Microsecond).Format(tFormat)
	n := len(meta) + len(msg) + len3 + len(t)
	j := make([]byte, n)
	o := copy(j, level)
	o += copy(j[o:], delim)
	o += copy(j[o:], t)
	o += copy(j[o:], delim)
	o += copy(j[o:], meta)
	o += copy(j[o:], delim)
	copy(j[o:], msg)
	return string(j)
}
