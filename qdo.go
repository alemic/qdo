package main

import (
	"flag"
	"os"
	"time"

	"github.com/borgenk/qdo/db"
	"github.com/borgenk/qdo/log"
	"github.com/borgenk/qdo/queue"
	_ "github.com/borgenk/qdo/web"
)

const Version = 0.1

const dbDefaultHost = "127.0.0.1"
const dbDefaultPort = 6379
const dbDefaultPass = ""
const dbDefaultIdx = 0

const qDefaultNWorkers = 5
const qDefaultTThrottle = time.Duration(time.Second / 2)
const qDefaultTTaskLimit = time.Duration(10 * time.Minute)
const qDefaultNTaskTries = 10

var Log = log.New(os.Stdout, "", 0)

func main() {
	host := flag.String("h", dbDefaultHost, "Database host")
	port := flag.Int("p", dbDefaultPort, "Database port")
	flag.Parse()

	Log.Infof("starting QDo %.1f", Version)

	dbc := db.Config{
		Host:        *host,
		Port:        *port,
		Pass:        dbDefaultPass,
		Idx:         dbDefaultIdx,
		Connections: qDefaultNWorkers + 3, // n workers + fetcher + scheduler + web
	}

	// Launch web admin interface server.
	//go web.Run(dbc)

	// Launch queue routines.
	qc := queue.Config{
		NWorker:      qDefaultNWorkers,
		Throttle:     qDefaultTThrottle,
		TaskTLimit:   qDefaultTTaskLimit,
		TaskMaxTries: qDefaultNTaskTries,
	}
	queue.Run(dbc, qc)
}
