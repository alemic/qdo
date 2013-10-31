package main

import (
	"flag"

	"github.com/borgenk/qdo/lib/log"
	"github.com/borgenk/qdo/lib/queue"
	"github.com/borgenk/qdo/lib/web"
)

const Version = "0.2.1"

const httpDefaultPort = 8080
const httpDefaultFilepath = "."
const dbDefaultFilepath = "qdo.db"

func main() {
	httpPort := flag.Int("p", httpDefaultPort, "HTTP port")
	httpDocumentRoot := flag.String("r", httpDefaultFilepath, "HTTP document root")
	dbFilepath := flag.String("f", dbDefaultFilepath, "Database file path")
	flag.Parse()

	log.Infof("starting QDo %s", Version)

	// Launch web admin interface server.
	go web.Run(*httpPort, *httpDocumentRoot)

	// Launch queue manager.
	queue.StartManager(*dbFilepath)
}
