package main

import (
	"flag"

	"github.com/borgenk/qdo/log"
	"github.com/borgenk/qdo/log/stdout"
	"github.com/borgenk/qdo/log/syslog"
	"github.com/borgenk/qdo/queue"
	"github.com/borgenk/qdo/web"
)

const Version = "0.2.1"

const defaultOptHttpPort = 8080
const defaultOptHttpFilepath = "."
const defaultOptDbFilepath = "qdo.db"
const defaultOptSyslog = false

func main() {
	optHttpPort := flag.Int("p", defaultOptHttpPort, "HTTP port")
	optHttpDocumentRoot := flag.String("r", defaultOptHttpFilepath, "HTTP document root")
	optDbFilepath := flag.String("f", defaultOptDbFilepath, "Database file path")
	optSyslog := flag.Bool("s", defaultOptSyslog, "Log to syslog")
	flag.Parse()

	// Setup logging method.
	if *optSyslog {
		w, err := syslog.New(syslog.LOG_LOCAL0, "qdo")
		if err != nil {
			panic("Unable to connect to syslog")
		}
		log.InitLog(w)
	} else {
		w := stdout.New()
		log.InitLog(w)
	}
	log.Infof("starting QDo %s", Version)

	// Launch web admin interface server.
	go web.Run(*optHttpPort, *optHttpDocumentRoot)

	// Launch queue manager.
	queue.StartManager(*optDbFilepath)
}
