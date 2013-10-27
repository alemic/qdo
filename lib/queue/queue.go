package queue

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/borgenk/qdo/lib/log"
)

type convSignal int

const (
	start  convSignal = iota
	pause  convSignal = iota
	resume convSignal = iota
	stop   convSignal = iota
)

type Conveyor struct {
	// Define resource.
	Object string `json:"object"`

	// Conveyor identification.
	ID string `json:"id"`

	// Conveyor created timestamp.
	Created time.Time `json:"created"`

	// Conveyor changed timestamp.
	Changed time.Time `json:"changed"`

	// Conveyor is in pause state.
	Paused bool `json:"paused"`

	// Conveyor configurations.
	Config Config `json:"config"`

	// Scheduler.
	Scheduler *Scheduler `json:"scheduler"`

	// Generate new task id by reading from channel.
	newTaskId chan string `json:"-"`

	// Limit number of simultaneous workers processing tasks.
	notifyReady chan int `json:"-"`

	// Conveyor status signal.
	notifySignal chan convSignal `json:"-"`

	cond *sync.Cond
}

type Config struct {
	// Number of simultaneous workers processing tasks.
	NWorker int32 `json:"n_worker"`

	// Number of maxium task invocations from queue per second.
	Throttle int32 `json:"throttle"`

	// Duration allowed per task to complete in seconds.
	TaskTLimit int32 `json:"task_t_limit"`

	// Number of tries per task before giving up. Set 0 for unlimited retries.
	TaskMaxTries int32 `json:"task_max_tries"`

	// Number of max log entries.
	LogSize int32 `json:"log_size"`
}

type Statistic struct {
	Object                    string        `json:"object"`
	InQueue                   int64         `json:"in_queue"`
	InProcessing              int64         `json:"in_processing"`
	InScheduled               int64         `json:"in_scheduled"`
	TotalProcessed            int           `json:"total_processed"`
	TotalProcessedOK          int           `json:"total_processed_ok"`
	TotalProcessedError       int           `json:"total_processed_error"`
	TotalProcessedRescheduled int           `json:"total_processed_rescheduled"`
	AvgTime                   time.Duration `json:"avg_time"`
	AvgTimeRecent             time.Duration `json:"avg_time_recent"`
}

type Task struct {
	Object  string `json:"object"`
	ID      string `json:"id"`
	Target  string `json:"target"`
	Payload string `json:"payload"`
	Tries   int32  `json:"tries"`
	Delay   int32  `json:"delay"`
}

func GobEncode(t *Task) ([]byte, error) {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(t)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func GobDecode(buf []byte) (*Task, error) {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	t := &Task{}
	err := decoder.Decode(t)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func NewConveyor(conveyorID string, config *Config) *Conveyor {
	now := time.Now()
	var locker sync.Mutex

	conv := &Conveyor{
		Object:       "conveyor",
		ID:           conveyorID,
		Created:      now,
		Changed:      now,
		Config:       *config,
		Paused:       false,
		newTaskId:    make(chan string),
		notifyReady:  make(chan int, config.NWorker),
		notifySignal: make(chan convSignal),
		cond:         sync.NewCond(&locker),
	}

	conv.Scheduler = NewScheduler(conv)

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
		conv.Config.NWorker)

	// Treat existing tasks in processing list as failed. Reschedule to waiting
	// queue.
	err := conv.reset()
	if err != nil {
		return err
	}

	// Start scheduler for delayed or rescheduled tasks.
	go conv.Scheduler.Start()

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
			log.Debug("sleeping")
			time.Sleep(1 * time.Second)
			continue
		}

		// Block until conveyor is ready to process next task.
		conv.notifyReady <- 1

		// Block until next task is recieved (waiting if queue is empty).
		task := conv.next()
		go conv.process(task)

		// Throttle task invocations per second.
		if conv.Config.Throttle > 0 {
			time.Sleep(time.Duration(
				time.Second / (time.Duration(conv.Config.Throttle) * time.Second)))
		}
	}
	return nil
}

func (conv *Conveyor) next() *Task {
	conv.cond.L.Lock()

start:
	iter := db.NewIterator(nil)
	iter.Seek([]byte("q\x00"))

	k := iter.Key()
	if iter.Valid() == false || comparer.DefaultComparer.Compare(k, []byte("q\xff")) > 0 {
		iter.Release()
		conv.cond.Wait()
		goto start
	}

	v := iter.Value()
	err := db.Delete(k, nil)
	if err != nil {
		log.Error(fmt.Sprintf("error occur while trying to delete key %s from db", k), err)
		panic("for now")
	}
	t, err := GobDecode(v)
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

	// TODO: increase statsTotal
	// conv.Stats.IncrTotal()

	_, err := url.Parse(task.Target)
	if err != nil {
		// Assume invalid task, discard it.
		log.Error("invalid target URL, discarding task", err)
		return
	}

	log.Infof("processing task id: %s target: %s tries: %d",
		task.ID, task.Target, task.Tries)

	transport := http.Transport{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, time.Duration(conv.Config.TaskTLimit)*time.Second)
		},
		Proxy: http.ProxyFromEnvironment,
		ResponseHeaderTimeout: time.Duration(conv.Config.TaskTLimit) * time.Second,
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
		log.Infof("task completed successfully: %s", resp.Status)

		endTime := time.Now()
		_ = int(endTime.Sub(startTime))
		//c.Do("INCRBY", conv.Redis.StatsTotalTime, strconv.Itoa(int(endTime.Sub(startTime))))
		return
	} else if err == nil && resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		log.Infof("task failed, request invalid: %s", resp.Status)
		return
	}

	if conv.Config.TaskMaxTries > 0 && task.Tries >= conv.Config.TaskMaxTries-1 {
		// Remove from ProcessingList.
		log.Infof("task reached max tries: %d", task.Tries)
		return
	}

	// TODO: increment statsTotalError
	//conv.Stats.IncrTotalError()

	delay, err := conv.Scheduler.Reschedule(task)
	if err != nil {
		log.Info("task failed")
		return
	}

	log.Infof("task failed, rescheduled for retry in %d seconds", delay)
	// TODO: increment StatsTotalRescheduled
	//conv.Stats.IncrTotalRescheduled()
}

func (conv *Conveyor) reset() error {
	// TODO: move from key range p to q.
	return nil
}

func (conv *Conveyor) Flush() error {
	// TODO: remove all keys in range q.
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

func (conv *Conveyor) Add(target, payload string) (*Task, error) {
	task := &Task{
		Object:  "task",
		ID:      <-conv.newTaskId,
		Target:  target,
		Payload: payload,
		Tries:   0,
		Delay:   0,
	}

	b, err := GobEncode(task)
	if err != nil {
		log.Error("unable to encode task", err)
		return nil, err
	}

	err = conv.add(task.ID, b)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (conv *Conveyor) add(taskId string, task []byte) error {
	conv.cond.L.Lock()
	defer conv.cond.L.Unlock()

	// Key format: q\x00[timestamp]\x00[taskid]
	key := fmt.Sprintf("q\x00%d\x00%s", time.Now().Unix(), taskId)
	wo := &opt.WriteOptions{}
	err = db.Put([]byte(key), task, wo)
	if err != nil {
		log.Error("add task to db failed", err)
		return err
	}

	// Signal new task to queue reader.
	conv.cond.Signal()

	return nil
}

func (conv *Conveyor) Stats() (*Statistic, error) {
	stats := &Statistic{
		Object: "statistic",
	}
	return stats, nil
}

func (conv *Conveyor) Tasks() ([]*Task, error) {
	i := 0
	limit := 100
	res := make([]*Task, 0, limit)

	start := []byte("q\x00")
	iter := db.NewIterator(nil)
	for iter.Seek(start); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()

		if comparer.DefaultComparer.Compare(k, []byte("q\xff")) > 0 {
			break
		}

		t, err := GobDecode(v)
		if err != nil {
			log.Error("", err)
			iter.Release()
			return res, err
		}
		res[i] = t

		i++
		if i >= limit {
			break
		}
	}
	iter.Release()

	return res, nil
}
