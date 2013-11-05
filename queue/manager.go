package queue

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb"
	"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb/comparer"

	"github.com/borgenk/qdo/log"
)

type Manager struct {
	Conveyors map[string]*Conveyor
}

var (
	manager *Manager
	db      *leveldb.DB
	mu      sync.Mutex
	err     error
)

// StartManager starts the conveyor state manager.
func StartManager(dbFilepath string) error {
	manager = &Manager{
		Conveyors: make(map[string]*Conveyor),
	}
	return manager.start(dbFilepath)
}

func (man *Manager) start(dbFilepath string) error {
	db, err = leveldb.OpenFile(dbFilepath, nil)
	if err != nil {
		log.Error(fmt.Sprintf("open database file %s failed", dbFilepath), err)
		return err
	}
	defer db.Close()

	storedConveyors, err := getAllStoredConveyors()
	if err != nil {
		log.Error("", err)
		return err
	}
	for _, v := range storedConveyors {
		man.Conveyors[v.ID] = v
		go man.Conveyors[v.ID].Init().Start()

	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	<-exit

	return nil
}

func getAllStoredConveyors() ([]*Conveyor, error) {
	res := make([]*Conveyor, 0, 1000)
	iter := db.NewIterator(nil)
	defer iter.Release()
	for iter.Seek([]byte("c\x00")); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()

		if comparer.DefaultComparer.Compare(k, []byte("c\xff")) > 0 {
			break
		}

		c := &Conveyor{}
		err := GobDecode(v, c)
		if err != nil {
			panic("for now")
		}

		res = append(res, c)
	}
	return res, nil
}

// GetConveyor returns a container for a given id if it exist.
func GetConveyor(conveyorID string) (*Conveyor, error) {
	mu.Lock()
	defer mu.Unlock()

	if manager == nil {
		return nil, errors.New("Manager not initialized")
	}

	conv, ok := manager.Conveyors[conveyorID]
	if !ok {
		return nil, errors.New("Conveyor not found")
	}
	return conv, nil
}

// GetALlConveyors returns all conveyors.
func GetAllConveyors() ([]*Conveyor, error) {
	mu.Lock()
	defer mu.Unlock()

	if manager == nil {
		return nil, errors.New("Manager not initialized")
	}

	res := make([]*Conveyor, 0, len(manager.Conveyors))
	for _, v := range manager.Conveyors {
		res = append(res, v)
	}
	return res, nil
}

// AddConveyor adds a new container with a given id and config setup.
// Conveyor will automatically start.
func AddConveyor(conveyorID string, config *Config) error {
	mu.Lock()
	defer mu.Unlock()

	if manager == nil {
		return errors.New("Manager not initialized")
	}

	conv := NewConveyor(conveyorID, config)
	manager.Conveyors[conveyorID] = conv

	b, err := GobEncode(conv)
	if err != nil {
		log.Error("", err)
		return err
	}
	err = db.Put(conveyorKey(conveyorID), b, nil)
	if err != nil {
		log.Error("", err)
		return err
	}

	go func() {
		manager.Conveyors[conveyorID].Start()
	}()
	return nil
}

// RemoveConveyor stops and removes the conveyor. The conveyor will wait on
// running tasks to complete before shutting down.
func RemoveConveyor(conveyorId string) error {
	mu.Lock()
	defer mu.Unlock()

	if manager == nil {
		return errors.New("Manager not initialized")
	}

	// Check if conveyor exist.
	_, ok := manager.Conveyors[conveyorId]
	if !ok {
		return errors.New("Conveyor does not exist")
	}

	err := db.Delete(conveyorKey(conveyorId), nil)
	if err != nil {
		return err
	}

	go func() {
		manager.Conveyors[conveyorId].Stop()
		delete(manager.Conveyors, conveyorId)
		// TODO: Clean up tasks.
	}()
	return nil
}

func conveyorKey(conveyorId string) []byte {
	return []byte(fmt.Sprintf("c\x00%s", conveyorId))
}
