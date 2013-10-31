package queue

import (
	"bytes"
	"crypto/sha1"
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
	scheduler     *Scheduler      `json:"-"`       // Scheduler.
	newTaskId     chan string     `json:"-"`       // Generate new task id by reading from channel.
	notifyReady   chan int        `json:"-"`       // Limit number of simultaneous workers processing tasks.
	notifySignal  chan convSignal `json:"-"`       // Conveyor status signal.
	cond          *sync.Cond      `json:"-"`
	queueKeyStart []byte          `json:"-"`
	queueKeyStop  []byte          `json:"-"`
	waitKeyStart  []byte          `json:"-"`
	waitKeyStop   []byte          `json:"-"`
}

type Config struct {
	NWorker      int32 `json:"n_worker"`       // Number of simultaneous workers processing tasks.
	Throttle     int32 `json:"throttle"`       // Number of maxium task invocations from queue per second.
	TaskTLimit   int32 `json:"task_t_limit"`   // Duration allowed per task to complete in seconds.
	TaskMaxTries int32 `json:"task_max_tries"` // Number of tries per task before giving up. Set 0 for unlimited retries.
	LogSize      int32 `json:"log_size"`       // Number of max log entries.
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
	Object    string `json:"object"`
	ID        string `json:"id"`
	Target    string `json:"target"`
	Payload   string `json:"payload"`
	Tries     int32  `json:"tries"`
	Delay     int32  `json:"delay"`
	Recurring int32  `json:"recurring"`
}

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

func (conv *Conveyor) Init() *Conveyor {
	conv.newTaskId = make(chan string)
	conv.notifyReady = make(chan int, conv.Config.NWorker)
	conv.notifySignal = make(chan convSignal)
	conv.scheduler = NewScheduler(conv)

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
		conv.Config.NWorker)

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
	iter.Seek(conv.queueKeyStart)

	k := iter.Key()
	if iter.Valid() == false || comparer.DefaultComparer.Compare(k, conv.queueKeyStop) > 0 {
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

	delay, err := conv.scheduler.Reschedule(task)
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

func (conv *Conveyor) Add(target, payload string, scheduled, recurring int64) (*Task, error) {
	task := &Task{
		Object:    "task",
		ID:        <-conv.newTaskId,
		Target:    target,
		Payload:   payload,
		Tries:     0,
		Delay:     0,
		Recurring: 0,
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
	log.Infof("new task %s added to conveyor %s", taskId, conv.ID)

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

	iter := db.NewIterator(nil)
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
