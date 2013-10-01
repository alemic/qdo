package queue

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/borgenk/qdo/third_party/github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/lib/db"
	"github.com/borgenk/qdo/lib/log"
)

const PendingQueueList = "qdo:manager:pendingqueuelist"
const ActiveQueueList = "qdo:manager:activequeuelist"

type Manager struct {
	PendingList string
	ActiveList  string
	Conveyors   map[string]*Conveyor
}

var manager *Manager

func StartManager() error {
	manager = &Manager{
		PendingList: PendingQueueList,
		ActiveList:  ActiveQueueList,
		Conveyors:   make(map[string]*Conveyor),
	}
	return manager.Start()
}

func (man *Manager) Start() error {
	// Reset all active conveyors.
	err := man.Reset()
	if err != nil {
		return err
	}

	// Discover new conveyors.
	man.Discover()
	return nil
}

func (man *Manager) Reset() error {
	for {
		c := db.Pool.Get()
		s, err := c.Do("RPOPLPUSH", man.ActiveList, man.PendingList)
		if err != nil {
			c.Close()
			log.Error("", err)
			return err
		}
		if s == nil {
			// All done, processing is empty.
			c.Close()
			return nil
		}
		c.Close()
	}
}

func (man *Manager) Discover() {
	for {
		c := db.Pool.Get()
		b, err := redis.Bytes(c.Do("BRPOPLPUSH", man.PendingList, man.ActiveList, "0"))

		c.Close()
		if err != nil {
			log.Error("error while fetching new queue", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		conv := &Conveyor{}
		err = json.Unmarshal(b, conv)
		if err != nil {
			log.Error("invalid config format", err)
			continue
		}

		_, isNew := man.Conveyors[conv.ID]
		if isNew {
			// Do reload stuff..
		}

		man.Conveyors[conv.ID] = conv
		go conv.Start()
	}
}

func (man *Manager) Remove(data []byte) error {
	c := db.Pool.Get()
	defer c.Close()
	_, err := redis.Int(c.Do("LREM", man.ActiveList, "1", data))
	if err != nil {
		log.Error("", err)
		return err
	}
	return nil
}

func AddConveyor(conveyorID string, config *Config) error {
	conveyor := NewConveyor(conveyorID, config)
	conv, err := json.Marshal(conveyor)
	if err != nil {
		log.Error("", err)
		return err
	}

	if db.Pool == nil {
		return errors.New("Database not initialized")
	}

	c := db.Pool.Get()
	defer c.Close()

	_, err = redis.Int(c.Do("LPUSH", manager.PendingList, conv))
	if err != nil {
		log.Error("", err)
		return err
	}
	return nil
}

func GetAllConveyor() []*Conveyor {
	resp := make([]*Conveyor, 0, len(manager.Conveyors))
	for _, v := range manager.Conveyors {
		resp = append(resp, v)
	}
	return resp
}

func GetConveyor(id string) (*Conveyor, error) {
	if manager == nil {
		return nil, nil
	}

	conv, ok := manager.Conveyors[id]
	if !ok {
		return nil, nil
	}

	return conv, nil
}
