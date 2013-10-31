package queue

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb"
	"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb/comparer"

	"github.com/borgenk/qdo/lib/log"
)

type Manager struct {
	Conveyors map[string]*Conveyor
}

var (
	manager *Manager
	db      *leveldb.DB
	err     error
)

func StartManager() error {
	manager = &Manager{
		Conveyors: make(map[string]*Conveyor),
	}
	return manager.start()
}

func (man *Manager) start() error {
	dbFile := "/tmp/my.db"

	db, err = leveldb.OpenFile(dbFile, nil)
	if err != nil {
		log.Error(fmt.Sprintf("open database file %s failed", dbFile), err)
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

func getAllStoredConveyors() ([]*Conveyor, error) {
	res := make([]*Conveyor, 0, 1000)

	start := []byte("c\x00")
	iter := db.NewIterator(nil)
	for iter.Seek(start); iter.Valid(); iter.Next() {
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
	iter.Release()

	return res, nil
}

func GetAllConveyor() ([]*Conveyor, error) {
	// TODO: Implement locking.
	if manager == nil {
		return nil, errors.New("Manager not initialized")
	}

	res := make([]*Conveyor, 0, len(manager.Conveyors))
	for _, v := range manager.Conveyors {
		res = append(res, v)
	}
	return res, nil
}

func AddConveyor(conveyorID string, config *Config) error {
	// TODO: Implement locking.
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
	// TODO: Implement locking.
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
	}()

	return nil
}

func conveyorKey(conveyorId string) []byte {
	return []byte(fmt.Sprintf("c\x00%s", conveyorId))
}
