package main

import (
	"flag"

	"github.com/borgenk/qdo/lib/db"
	"github.com/borgenk/qdo/lib/log"
	"github.com/borgenk/qdo/lib/queue"
	"github.com/borgenk/qdo/lib/web"
)

const Version = 0.2

const dbDefaultHost = "127.0.0.1"
const dbDefaultPort = 6379
const dbDefaultPass = ""
const dbDefaultIdx = 0

const webDefaultPort = 8080
const webDefaultDocumentRoo = "/var/www/"

func main() {
	host := flag.String("h", dbDefaultHost, "Database host")
	port := flag.Int("p", dbDefaultPort, "Database port")
	pass := flag.String("P", dbDefaultPass, "Database password")
	idx := flag.Int("i", dbDefaultIdx, "Database index")
	webPort := flag.Int("w", webDefaultPort, "Web port")
	webDocumentRoot := flag.String("D", webDefaultDocumentRoo, "Web document root")
	flag.Parse()

	log.Infof("starting QDo %.1f", Version)

	dbc := db.Config{
		Host: *host,
		Port: *port,
		Pass: *pass,
		Idx:  *idx,
	}
	db.ConnectPool(dbc)

	// Launch web admin interface server.
	go web.Run(*webPort, *webDocumentRoot)

	// Launch queue manager.
	queue.StartManager()
}
