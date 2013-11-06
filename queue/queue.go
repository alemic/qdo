package queue

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/borgenk/qdo/log"
)

const (
	queueKey   string = "q"
	waitKey    string = "w"
	startPoint string = "\x00"
	stopPoint  string = "\xff"
)

type convSignal int

const (
	start  convSignal = iota
	pause  convSignal = iota
	resume convSignal = iota
	stop   convSignal = iota
)

type Conveyor struct {
	Object        string          `json:"object"`  // Define resource.
	ID            string          `json:"id"`      // Conveyor identification.
	Created       time.Time       `json:"created"` // Conveyor created timestamp.
	Changed       time.Time       `json:"changed"` // Conveyor changed timestamp.
	Paused        bool            `json:"paused"`  // Conveyor is in pause state.
	Config        Config          `json:"config"`  // Conveyor configurations.
	Stats         *Stats          `json:"-"`
	scheduler     *Scheduler      `json:"-"` // Scheduler.
	newTaskId     chan string     `json:"-"` // Generate new task id by reading from channel.
	notifyReady   chan int        `json:"-"` // Limit number of simultaneous workers processing tasks.
	notifySignal  chan convSignal `json:"-"` // Conveyor status signal.
	cond          *sync.Cond      `json:"-"`
	queueKeyStart []byte          `json:"-"`
	queueKeyStop  []byte          `json:"-"`
	waitKeyStart  []byte          `json:"-"`
	waitKeyStop   []byte          `json:"-"`
}

type Config struct {
	MaxConcurrent int32 `json:"max_concurrent"` // Number of simultaneous workers processing tasks.
	MaxRate       int32 `json:"max_rate"`       // Number of maxium task invocations from queue per second.
	TaskTimeout   int32 `json:"task_timeout"`   // Duration allowed per task to complete in seconds.
	TaskMaxTries  int32 `json:"task_max_tries"` // Number of tries per task before giving up. Set 0 for unlimited retries.
}

type AtomicInt int64

func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

func (i *AtomicInt) Set(n int64) {
	atomic.StoreInt64((*int64)(i), n)
}

func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

type Stats struct {
	InQueue                   AtomicInt
	InProcessing              AtomicInt
	InScheduled               AtomicInt
	TotalProcessed            AtomicInt
	TotalProcessedOK          AtomicInt
	TotalProcessedError       AtomicInt
	TotalProcessedRescheduled AtomicInt
	TotalTime                 AtomicInt
	TimeLastOK                AtomicInt
}

type Task struct {
	Object  string `json:"object"`
	ID      string `json:"id"`
	Target  string `json:"target"`
	Payload string `json:"payload"`
	Tries   int32  `json:"tries"`
	Delay   int32  `json:"delay"`
}

// NewConveyor creates a new conveyor ready to handle tasks after running
// initialize on itself.
func NewConveyor(conveyorID string, config *Config) *Conveyor {
	now := time.Now()
	conv := &Conveyor{
		Object:  "conveyor",
		ID:      conveyorID,
		Created: now,
		Changed: now,
		Config:  *config,
		Paused:  false,
	}
	conv.Init()
	return conv
}

// Init intializes either a new or restored conveyor.
func (conv *Conveyor) Init() *Conveyor {
	conv.newTaskId = make(chan string)
	conv.notifyReady = make(chan int, conv.Config.MaxConcurrent)
	conv.notifySignal = make(chan convSignal)
	conv.scheduler = NewScheduler(conv)

	conv.Stats = &Stats{}
	conv.queueKeyStart = []byte(conv.ID + startPoint + queueKey + startPoint)
	conv.queueKeyStop = []byte(conv.ID + startPoint + queueKey + stopPoint)
	conv.waitKeyStart = []byte(conv.ID + startPoint + waitKey + startPoint)
	conv.waitKeyStop = []byte(conv.ID + startPoint + waitKey + stopPoint)

	var locker sync.Mutex
	conv.cond = sync.NewCond(&locker)

	// http://blog.cloudflare.com/go-at-cloudflare
	go func() {
		h := sha1.New()
		c := []byte(time.Now().String())
		for {
			h.Write(c)
			conv.newTaskId <- fmt.Sprintf("%x", h.Sum(nil))
		}
	}()

	return conv
}

// Starting the conveyor belt and handles retrieving and delegating tasks.
func (conv *Conveyor) Start() error {
	log.Infof("starting conveyor \"%s\" with %d worker(s)", conv.ID,
		conv.Config.MaxConcurrent)

	// Treat existing tasks in processing list as failed. Reschedule to waiting
	// queue.
	err := conv.reset()
	if err != nil {
		return err
	}

	// Start scheduler for delayed or rescheduled tasks.
	go conv.scheduler.Start()

	for {
		select {
		case sig := <-conv.notifySignal:
			if sig == stop {
				log.Infof("stopping conveyor %s", conv.ID)
				return nil
			} else if sig == pause {
				log.Infof("pausing conveyor %s", conv.ID)
				conv.Paused = true
			} else if sig == resume {
				log.Infof("resuming conveyor %s", conv.ID)
				conv.Paused = false
			}
		default:
		}

		if conv.Paused {
			time.Sleep(1 * time.Second)
			continue
		}

		// Block until conveyor is ready to process next task.
		conv.notifyReady <- 1

		// Block until next task is recieved (waiting if queue is empty).
		task := conv.next()
		go conv.process(task)

		// Throttle task invocations per second.
		if conv.Config.MaxRate > 0 {
			time.Sleep(time.Duration(
				time.Second / (time.Duration(conv.Config.MaxRate) * time.Second)))
		}
	}
	return nil
}

func (conv *Conveyor) next() *Task {
	conv.cond.L.Lock()

start:
	iter := db.NewIterator(nil)
	iter.Seek(conv.queueKeyStart)

	k := iter.Key()
	if iter.Valid() == false || comparer.DefaultComparer.Compare(k, conv.queueKeyStop) > 0 {
		iter.Release()
		conv.cond.Wait()
		goto start
	}

	// TODO: implement queue delete func.
	v := iter.Value()
	err := db.Delete(k, nil)
	if err != nil {
		log.Error(fmt.Sprintf("error occur while trying to delete key %s from db", k), err)
		panic("for now")
	}
	conv.Stats.InQueue.Add(-1)

	t := &Task{}
	err = GobDecode(v, t)
	if err != nil {
		log.Error("unable to decode task from db", err)
		panic("for now")
	}

	iter.Release()
	conv.cond.L.Unlock()

	return t
}

func (conv *Conveyor) process(task *Task) {
	defer func() { <-conv.notifyReady }()

	startTime := time.Now()
	conv.Stats.TotalProcessed.Add(1)

	_, err := url.Parse(task.Target)
	if err != nil {
		// Assume invalid task, discard it.
		log.Error(fmt.Sprintf("conveyor %s task %s has invalid target URL, discarding", conv.ID, task.ID), err)
		return
	}

	log.Infof("conveyor %s task %s processing with target: %s tries: %d",
		conv.ID, task.ID, task.Target, task.Tries)

	transport := http.Transport{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, time.Duration(conv.Config.TaskTimeout)*time.Second)
		},
		Proxy: http.ProxyFromEnvironment,
		ResponseHeaderTimeout: time.Duration(conv.Config.TaskTimeout) * time.Second,
	}
	client := http.Client{
		Transport: &transport,
	}
	resp, err := client.Post(task.Target, "application/json",
		bytes.NewReader([]byte(task.Payload)))
	if err == nil {
		resp.Body.Close()
	}

	if err == nil && resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		// Consider task done successful.
		// TODO: Remove task from processing list.
		log.Infof("conveyor %s task %s completed successfully: %s", conv.ID, task.ID, resp.Status)
		conv.Stats.TotalProcessedOK.Add(1)
		timeSpent := int64(time.Now().Sub(startTime))
		conv.Stats.TotalTime.Add(timeSpent)
		conv.Stats.TimeLastOK.Set(timeSpent)
		return
	} else if err == nil && resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		// Consider task invalid as return status says. No point in retrying
		// later.
		// TODO: Remove task from processing list.
		log.Infof("conveyor %s task %s failed, request invalid: %s", conv.ID, task.ID, resp.Status)
		conv.Stats.TotalProcessedError.Add(1)
		return
	} else if err != nil {
		// Error while sending task request, retry again later.
		log.Error(fmt.Sprintf("conveyor %s task %s resulted in error", conv.ID, task.ID), err)
	} else {
		// Task sent, but not completed successfully. Retry again later.
		log.Infof("conveyor %s task %s failed with http status %d", conv.ID, task.ID, resp.StatusCode)
	}

	conv.Stats.TotalProcessedError.Add(1)

	if conv.Config.TaskMaxTries > 0 && task.Tries >= conv.Config.TaskMaxTries-1 {
		// TODO: Remove task from processing list.
		log.Infof("conveyor %s task %s reached max tries: %d", conv.ID, task.ID, task.Tries)
		return
	}

	delay, err := conv.scheduler.Reschedule(task)
	if err != nil {
		log.Error(fmt.Sprintf("conveyor %s task %s could not be rescheduled", conv.ID, task.ID), err)
		// TODO: try to dump task.
		return
	}

	log.Infof("conveyor %s task %s rescheduled for retry in %d seconds", conv.ID, task.ID, delay)
	conv.Stats.TotalProcessedRescheduled.Add(1)
}

func (conv *Conveyor) reset() error {
	// TODO: Move all tasks from processing list to waiting list.
	return nil
}

func (conv *Conveyor) Flush() error {
	// TODO: Remove all tasks in waiting list.
	return nil
}

func (conv *Conveyor) Stop() {
	if conv.notifySignal == nil {
		panic("nil notifySignal")
	}
	go func() { conv.notifySignal <- stop }()
}

func (conv *Conveyor) Pause() {
	if conv.notifySignal == nil {
		panic("nil notifySignal")
	}
	go func() { conv.notifySignal <- pause }()
}

func (conv *Conveyor) Resume() {
	if conv.notifySignal == nil {
		panic("nil notifySignal")
	}
	go func() { conv.notifySignal <- resume }()
}

func (conv *Conveyor) Add(target, payload string, scheduled int64) (*Task, error) {
	task := &Task{
		Object:  "task",
		ID:      <-conv.newTaskId,
		Target:  target,
		Payload: payload,
		Tries:   0,
		Delay:   0,
	}

	t, err := GobEncode(task)
	if err != nil {
		log.Error("unable to encode task", err)
		return nil, err
	}

	if scheduled > 0 {
		// Delayed task.
		err = conv.scheduler.Add(task.ID, t, scheduled)
		if err != nil {
			return nil, err
		}
	} else {
		// Normal task.
		err = conv.add(task.ID, t)
		if err != nil {
			return nil, err
		}
	}
	return task, nil
}

func (conv *Conveyor) add(taskId string, task []byte) error {
	conv.cond.L.Lock()
	defer conv.cond.L.Unlock()

	// Key format: [conv id] \x00 [key type] \x00 [timestamp] \x00 [task id]
	key := append(conv.queueKeyStart,
		[]byte(fmt.Sprintf("%d%s%s", time.Now().Unix(), startPoint, taskId))...)
	log.Infof("conveyor %s task %s added to conveyor", conv.ID, taskId)

	wo := &opt.WriteOptions{}
	err = db.Put([]byte(key), task, wo)
	if err != nil {
		log.Error("add task to db failed", err)
		return err
	}

	// Signal new task to queue reader.
	conv.cond.Signal()
	conv.Stats.InQueue.Add(1)
	return nil
}

func (conv *Conveyor) Tasks() ([]*Task, error) {
	i := 0
	limit := 100
	res := make([]*Task, 0, limit)

	iter := db.NewIterator(nil)
	defer iter.Release()
	for iter.Seek(conv.queueKeyStart); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()

		if comparer.DefaultComparer.Compare(k, conv.queueKeyStop) > 0 {
			break
		}

		t := &Task{}
		err := GobDecode(v, t)
		if err != nil {
			log.Error("", err)
			return res, err
		}
		res[i] = t

		i++
		if i >= limit {
			break
		}
	}

	return res, nil
}
