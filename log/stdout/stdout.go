package stdout

import (
	"fmt"
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

type Writer struct {
	logger *stdlib.Logger
}

func New() *Writer {
	return &Writer{stdlib.New(os.Stdout, "", 0)}
}

func (w *Writer) Debug(msg string) {
	w.logger.Println(join2(DEBUG, msg))
}

func (w *Writer) Debugf(format string, v ...interface{}) {
	w.logger.Println(join2(DEBUG, fmt.Sprintf(format, v...)))
}

func (w *Writer) Info(msg string) {
	w.logger.Println(join2(INFO, msg))
}

func (w *Writer) Infof(format string, v ...interface{}) {
	w.logger.Println(join2(INFO, fmt.Sprintf(format, v...)))
}

func (w *Writer) Warn(msg string) {
	w.logger.Println(join2(WARN, msg))
}

func (w *Writer) Warnf(format string, v ...interface{}) {
	w.logger.Println(join2(WARN, fmt.Sprintf(format, v...)))
}

func (w *Writer) Error(meta string, e error) {
	w.logger.Println(join3(ERROR, meta, e.Error()))
}
func (w *Writer) Errorf(format string, v ...interface{}) {
	w.logger.Println(join2(ERROR, fmt.Sprintf(format, v...)))
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
