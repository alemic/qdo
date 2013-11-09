package log

var w *Writer

func InitLog(implementation Writer) {
	w = &implementation
}

type Writer interface {
	Debug(msg string)
	Debugf(format string, v ...interface{})
	Info(msg string)
	Infof(format string, v ...interface{})
	Warn(msg string)
	Warnf(format string, v ...interface{})
	Error(meta string, e error)
	Errorf(format string, v ...interface{})
}

func Debug(msg string) {
	(*w).Debug(msg)
}

func Debugf(format string, v ...interface{}) {
	(*w).Debugf(format, v...)
}

func Info(msg string) {
	(*w).Info(msg)
}

func Infof(format string, v ...interface{}) {
	(*w).Infof(format, v...)
}

func Warn(msg string) {
	(*w).Warn(msg)
}

func Warnf(format string, v ...interface{}) {
	(*w).Warnf(format, v...)
}

func Error(meta string, e error) {
	(*w).Error(meta, e)
}
func Errorf(format string, v ...interface{}) {
	(*w).Errorf(format, v...)
}
