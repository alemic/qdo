package db

import (
	"fmt"

	"github.com/borgenk/qdo/third_party/github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/lib/log"
)

type Config struct {
	Host string
	Port int
	Pass string
	Idx  int
}

var Pool *redis.Pool

func ConnectPool(dbc Config) {
	Pool = &redis.Pool{
		MaxActive:   100,
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
