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

const ManagerConveyorKey = "qdo:manager:conveyor"

type Manager struct {
	ConveyorsKey string
	Conveyors    map[string]*Conveyor
}

var manager *Manager

func StartManager() error {
	manager = &Manager{
		ConveyorsKey: ManagerConveyorKey,
		Conveyors:    make(map[string]*Conveyor),
	}
	return manager.Start()
}

func (man *Manager) Start() error {
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

func (man *Manager) RemoveConveyor(conveyorID string) error {
	return nil
}

func GetAllConveyor() []*Conveyor {
	resp := make([]*Conveyor, 0, len(manager.Conveyors))
	for _, v := range manager.Conveyors {
		resp = append(resp, v)
	}
	return resp
}

func GetConveyor(conveyorID string) (*Conveyor, error) {
	if manager == nil {
		return nil, nil
	}

	conv, ok := manager.Conveyors[conveyorID]
	if !ok {
		return nil, nil
	}

	return conv, nil
}
