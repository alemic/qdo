package db

import (
	"fmt"

	"github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/log"
)

const WaitingList = "waitinglist"
const ProcessingList = "processinglist"
const Schedulelist = "schedulelist"
const ScheduleId = "scheduleid"

type Config struct {
	Host        string // Default database host.
	Port        int    // Default database port.
	Pass        string // Default database password.
	Idx         int    // Default database index.
	Connections int
}

var Pool *redis.Pool

func ConnectPool(dbc Config) {
	Pool = &redis.Pool{
		MaxActive:   dbc.Connections,
		MaxIdle:     dbc.Connections,
		IdleTimeout: 0,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", dbc.Host, dbc.Port))
			if err != nil {
				log.Error("", err)
				return nil, err
			}
			if dbc.Pass != "" {
				_, err = c.Do("AUTH", dbc.Pass)
				if err != nil {
					c.Close()
					log.Error("", err)
					return nil, err
				}
			}
			_, err = c.Do("SELECT", dbc.Idx)
			if err != nil {
				log.Error("", err)
				return nil, err
			}
			return c, err
		},
	}
}
