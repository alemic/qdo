package main

import (
	"flag"

	"github.com/borgenk/qdo/lib/log"
	"github.com/borgenk/qdo/lib/queue"
	"github.com/borgenk/qdo/lib/web"
)

const Version = "0.2.0"

const webDefaultPort = 8080
const webDefaultDocumentRoo = "/var/www/"

func main() {
	webPort := flag.Int("w", webDefaultPort, "Web port")
	webDocumentRoot := flag.String("d", webDefaultDocumentRoo, "Web document root")
	flag.Parse()

	log.Infof("starting QDo %s", Version)

	// Launch web admin interface server.
	go web.Run(*webPort, *webDocumentRoot)

	// Launch queue manager.
	queue.StartManager()
}
