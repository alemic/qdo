package queue

import (
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/borgenk/qdo/third_party/github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/lib/db"
	"github.com/borgenk/qdo/lib/log"
)

type Manager struct {
	ConveyorsKey string
	Conveyors    map[string]*Conveyor
}

var manager *Manager

func StartManager() error {
	manager = &Manager{
		ConveyorsKey: "qdo:manager:conveyor",
		Conveyors:    make(map[string]*Conveyor),
	}
	return manager.start()
}

func (man *Manager) start() error {
	if db.Pool == nil {
		return errors.New("Database not initialized")
	}
	c := db.Pool.Get()
	defer c.Close()

	reply, err := redis.Strings(c.Do("HGETALL", man.ConveyorsKey))
	if err != nil {
		log.Error("", err)
		return err
	}

	key := ""
	for _, v := range reply {
		if key == "" {
			key = v
		} else {
			man.Conveyors[key] = &Conveyor{}
			err = json.Unmarshal([]byte(v), man.Conveyors[key])
			if err != nil {
				log.Error("", err)
				return err
			}
			go man.Conveyors[key].Start()

			key = ""
		}
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	<-exit

	return nil
}

func GetConveyor(conveyorID string) (*Conveyor, error) {
	if manager == nil {
		return nil, errors.New("Manager not initialized")
	}

	conv, ok := manager.Conveyors[conveyorID]
	if !ok {
		return nil, nil
	}

	return conv, nil
}

func GetAllConveyor() []*Conveyor {
	if manager == nil {
		return nil
	}

	resp := make([]*Conveyor, 0, len(manager.Conveyors))
	for _, v := range manager.Conveyors {
		resp = append(resp, v)
	}
	return resp
}

func AddConveyor(conveyorID string, config *Config) error {
	// TODO: Implement locking.
	manager.Conveyors[conveyorID] = NewConveyor(conveyorID, config)
	go manager.Conveyors[conveyorID].Start()

	b, err := json.Marshal(manager.Conveyors[conveyorID])
	if err != nil {
		log.Error("", err)
		return err
	}

	if db.Pool == nil {
		return errors.New("Database not initialized")
	}
	c := db.Pool.Get()
	defer c.Close()

	_, err = redis.Int(c.Do("HSET", manager.ConveyorsKey, conveyorID, b))
	if err != nil {
		log.Error("", err)
		return err
	}
	return nil
}

func RemoveConveyor(conveyorID string) error {
	if manager == nil {
		return errors.New("Manager not initialized")
	}

	conv, ok := manager.Conveyors[conveyorID]
	if !ok {
		return errors.New("Conveyor does not exist")
	}

	if db.Pool == nil {
		err := errors.New("Database not initialized")
		log.Error("", err)
		return err
	}
	c := db.Pool.Get()
	defer c.Close()

	_, err := redis.Int(c.Do("HDEL", manager.ConveyorsKey, conv.ID))
	if err != nil {
		return err
	}

	go conv.Stop()

	return nil
}
