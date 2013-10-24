package queue

import (
	"bytes"
	"container/list"
	"crypto/sha1"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	//"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb"
	//"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb/opt"

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

	// Conveyor items.
	tasks *list.List `json:"-"`

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
		Scheduler:    NewScheduler(conveyorID),
		tasks:        list.New(),
		newTaskId:    make(chan string),
		notifyReady:  make(chan int, config.NWorker),
		notifySignal: make(chan convSignal),
		cond:         sync.NewCond(&locker),
	}

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
		log.Debug("wating on notify ready")
		log.Debug("waiting on new task")
		conv.notifyReady <- 1

		task := conv.Next()

		go conv.process(task)

		// Throttle task invocations per second.
		if conv.Config.Throttle > 0 {
			time.Sleep(time.Duration(
				time.Second / (time.Duration(conv.Config.Throttle) * time.Second)))
		}
	}
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
	// Reload tasks here?
	return nil
}

func (conv *Conveyor) Flush() error {
	// Delete queuelogfile
	return nil
}

func (conv *Conveyor) Add(target, payload string) *Task {
	task := &Task{
		Object:  "task",
		ID:      <-conv.newTaskId,
		Target:  target,
		Payload: payload,
		Tries:   0,
		Delay:   0,
	}

	conv.cond.L.Lock()
	defer conv.cond.L.Unlock()
	conv.tasks.PushBack(task)
	conv.cond.Signal()

	return task
}

func (conv *Conveyor) Next() *Task {
	conv.cond.L.Lock()
start:
	e := conv.tasks.Front()
	if e == nil {
		conv.cond.Wait()
		goto start
	}
	conv.tasks.Remove(e)
	conv.cond.L.Unlock()
	return e.Value.(*Task)
}

func (conv *Conveyor) Stats() (*Statistic, error) {
	stats := &Statistic{
		Object: "statistic",
	}

	/*
		c.Send("MULTI")
		c.Send("LLEN", conv.Redis.WaitingList)
		c.Send("LLEN", conv.Redis.ProcessingList)
		c.Send("ZCARD", conv.Scheduler.ScheduleList)
		c.Send("GET", conv.Redis.StatsTotal)
		c.Send("GET", conv.Redis.StatsTotalOK)
		c.Send("GET", conv.Redis.StatsTotalRescheduled)
		c.Send("GET", conv.Redis.StatsTotalError)
		c.Send("GET", conv.Redis.StatsTotalTime)
		c.Send("GET", conv.Redis.StatsAvgTimeRecent)
		reply, err := redis.Values(c.Do("EXEC"))
		if err != nil {
			log.Error("", err)
			return nil, err
		}

		if reply[0] != nil {
			stats.InQueue, _ = reply[0].(int64)
			if err != nil {
				log.Error("", err)
				return nil, err
			}
		}
		if reply[1] != nil {
			stats.InProcessing, _ = reply[1].(int64)
			if err != nil {
				log.Error("", err)
				return nil, err
			}
		}
		if reply[2] != nil {
			stats.InScheduled, _ = reply[2].(int64)
			if err != nil {
				log.Error("", err)
				return nil, err
			}
		}
		if reply[3] != nil {
			stats.TotalProcessed, err = strconv.Atoi(string(reply[3].([]byte)))
			if err != nil {
				log.Error("", err)
				return nil, err
			}
		}
		if reply[4] != nil {
			stats.TotalProcessedOK, err = strconv.Atoi(string(reply[4].([]byte)))
			if err != nil {
				log.Error("", err)
				return nil, err
			}
		}
		if reply[5] != nil {
			stats.TotalProcessedRescheduled, err = strconv.Atoi(string(reply[5].([]byte)))
			if err != nil {
				log.Error("", err)
				return nil, err
			}
		}
		if reply[6] != nil {
			stats.TotalProcessedError, err = strconv.Atoi(string(reply[6].([]byte)))
			if err != nil {
				log.Error("", err)
				return nil, err
			}
		}
		if reply[7] != nil {
			v, err := strconv.Atoi(string(reply[7].([]byte)))
			if err != nil {
				log.Error("", err)
				return nil, err
			}
			stats.AvgTime = time.Duration(int(time.Duration(v)) / stats.TotalProcessedOK)
		}
		if reply[8] != nil {
			v, err := strconv.Atoi(string(reply[8].([]byte)))
			if err != nil {
				log.Error("", err)
				return nil, err
			}
			stats.AvgTimeRecent = time.Duration(v)
		}*/

	return stats, nil
}

func (conv *Conveyor) Tasks() []*Task {
	conv.cond.L.Lock()
	defer conv.cond.L.Unlock()

	limit := 100

	res := make([]*Task, 0, limit)
	i := 0
	for e := conv.tasks.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
		res = append(res, e.Value.(*Task))
		i++
		if i >= limit {
			break
		}
	}
	return res
}
