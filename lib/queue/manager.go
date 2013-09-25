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
	Conveyors   []*Conveyor
}

var manager *Manager

func StartManager() error {
	manager = &Manager{
		PendingList: PendingQueueList,
		ActiveList:  ActiveQueueList,
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
		man.ActivateConveyor(b)
	}
}

func (man *Manager) ActivateConveyor(data []byte) {
	conv := &Conveyor{}
	err := json.Unmarshal(data, conv)
	if err != nil {
		log.Error("invalid config format", err)
		man.Remove(data)
		return
	}
	man.Conveyors = append(man.Conveyors, conv)
	go conv.Start()
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

func GetAllConveyor() ([]Conveyor, error) {
	if db.Pool == nil {
		return nil, errors.New("Database not initialized")
	}
	c := db.Pool.Get()
	defer c.Close()

	reply, err := redis.Values(c.Do("LRANGE", manager.ActiveList, "0", "-1"))
	if err != nil {
		log.Error("", err)
		return nil, err
	}

	// Make a new slice of equal length as result. Type assert to []byte and
	// JSON decode into slice element.
	resp := make([]Conveyor, len(reply))
	for i, v := range reply {
		err = json.Unmarshal(v.([]byte), &resp[i])
		if err != nil {
			log.Error("", err)
			return nil, err
		}
	}
	return resp, nil
}

func GetConveyor(id string) (*Conveyor, error) {
	if db.Pool == nil {
		return nil, errors.New("Database not initialized")
	}
	c := db.Pool.Get()
	defer c.Close()

	conveyors, err := GetAllConveyor()
	if err != nil {
		return nil, err
	}

	for _, val := range conveyors {
		if val.ID == id {
			return &val, nil
		}
	}
	return nil, nil
}
