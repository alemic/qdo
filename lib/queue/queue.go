package queue

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/borgenk/qdo/third_party/github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/lib/db"
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

	// Limit number of simultaneous workers processing tasks.
	notifyReady chan int `json:"-"`

	// Conveyor status signal.
	notifySignal chan convSignal `json:"-"`

	// Conveyor configurations.
	Config Config `json:"config"`

	// Redis keys.
	Redis *RedisKeys `json:"-"`

	// Scheduler.
	Scheduler *Scheduler `json:"scheduler"`
}

type RedisKeys struct {
	// Task ID key.
	TaskIdKey string `json:"task_id_key"`

	// Conveyor waiting list name.
	WaitingList string `json:"waiting_list"`

	// Conveyor processing list name.
	ProcessingList string `json:"processing_list"`

	// Name of log message list.
	LogMessageList string `json:"log_message_list"`

	// Name of statistic key: total tasks processed.
	StatsTotal string `json:"stats_total"`

	// Name of statistic key: total tasks processed ok.
	StatsTotalOK string `json:"stats_total_ok"`

	// Name of statistic key: total tasks rescheduled.
	StatsTotalRescheduled string `json:"stats_total_rescheduled"`

	// Name of statistic key: total tasks processed with error.
	StatsTotalError string `json:"stats_total_error"`

	// Name of statistic key: average time spent per task.
	StatsTotalTime string `json:"stats_total_time"`

	// Name of statistic key: average time spent recently per task.
	StatsAvgTimeRecent string `json:"stats_av_time_recent"`
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
	ID      int    `json:"id"`
	Target  string `json:"target"`
	Payload string `json:"payload"`
	Tries   int32  `json:"tries"`
	Delay   int32  `json:"delay"`
}

func NewConveyor(conveyorID string, config *Config) *Conveyor {
	now := time.Now()
	conv := &Conveyor{
		Object:    "conveyor",
		ID:        conveyorID,
		Created:   now,
		Changed:   now,
		Config:    *config,
		Paused:    false,
		Scheduler: NewScheduler(conveyorID),
		Redis:     NewRedisKeys(conveyorID),
	}
	return conv
}

func NewRedisKeys(conveyorID string) *RedisKeys {
	key := &RedisKeys{
		TaskIdKey:             "qdo:" + conveyorID + ":queue:task:next_id",
		WaitingList:           "qdo:" + conveyorID + ":queue:waitinglist",
		ProcessingList:        "qdo:" + conveyorID + ":queue:processinglist",
		StatsTotal:            "qdo:" + conveyorID + ":stat:total",
		StatsTotalOK:          "qdo:" + conveyorID + ":stat:totalok",
		StatsTotalRescheduled: "qdo:" + conveyorID + ":stat:rescheduled",
		StatsTotalError:       "qdo:" + conveyorID + ":stat:totalerror",
		StatsTotalTime:        "qdo:" + conveyorID + ":stat:totaltime",
		StatsAvgTimeRecent:    "qdo:" + conveyorID + ":stat:avgtimerecent",
	}
	return key
}

func (conv *Conveyor) Start() error {
	log.Infof("starting conveyor \"%s\" with %d worker(s)", conv.ID,
		conv.Config.NWorker)

	conv.notifyReady = make(chan int, conv.Config.NWorker)
	conv.notifySignal = make(chan convSignal)

	if conv.Redis == nil {
		conv.Redis = NewRedisKeys(conv.ID)
	}

	// Treat existing tasks in processing list as failed. Reschedule to waiting
	// queue. Also makes sure we have database connection before we go any
	// further.
	err := conv.reset()
	if err != nil {
		return err
	}

	// Start scheduler for delayed or rescheduled tasks.
	go conv.Scheduler.Start(conv.Redis.WaitingList)

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
		conv.notifyReady <- 1

		// Block until Redis sends next element from list.
		log.Debug("getting new db connection")
		c := db.Pool.Get()
		log.Debug("waiting on new task")
		b, err := redis.Bytes(c.Do("BRPOPLPUSH", conv.Redis.WaitingList, conv.Redis.ProcessingList, "5"))
		c.Close()
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			<-conv.notifyReady
			continue
		}

		go conv.process(b)

		// Throttle task invocations per second.
		if conv.Config.Throttle > 0 {
			time.Sleep(time.Duration(time.Second / (time.Duration(conv.Config.Throttle) * time.Second)))
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

func (conv *Conveyor) process(data []byte) {
	defer func() { <-conv.notifyReady }()

	startTime := time.Now()

	c := db.Pool.Get()
	defer c.Close()

	c.Do("INCR", conv.Redis.StatsTotal)

	task := &Task{}
	err := json.Unmarshal(data, task)
	if err != nil {
		// Assume invalid task, discard it.
		log.Error("invalid task format", err)
		conv.removeProcessing(&c, data, "error")
		return
	}

	_, err = url.Parse(task.Target)
	if err != nil {
		// Assume invalid task, discard it.
		log.Error("invalid target URL, discarding task", err)
		conv.removeProcessing(&c, data, "error")
		return
	}

	log.Infof("processing task id: %d target: %s tries: %d",
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
		conv.removeProcessing(&c, data, "ok")
		log.Infof("task completed successfully: %s", resp.Status)

		endTime := time.Now()
		c.Do("INCRBY", conv.Redis.StatsTotalTime, strconv.Itoa(int(endTime.Sub(startTime))))
		return
	} else if err == nil && resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		conv.removeProcessing(&c, data, "error")
		log.Infof("task failed, request invalid: %s", resp.Status)
		return
	}

	if conv.Config.TaskMaxTries > 0 && task.Tries >= conv.Config.TaskMaxTries-1 {
		// Remove from ProcessingList.
		log.Infof("task reached max tries: %d", task.Tries)
		conv.removeProcessing(&c, data, "error")
		return
	}

	c.Do("INCR", conv.Redis.StatsTotalError)

	delay, err := conv.Scheduler.Reschedule(conv.Redis.ProcessingList, task, data)
	if err != nil {
		log.Info("task failed")
		return
	}

	log.Infof("task failed, rescheduled for retry in %d seconds", delay)
	c.Do("INCR", conv.Redis.StatsTotalRescheduled)
}

func (conv *Conveyor) reset() error {
	for {
		c := db.Pool.Get()
		err := c.Err()
		if err != nil {
			log.Error("", err)
			time.Sleep(5 * time.Second)
		} else {
			s, err := c.Do("RPOPLPUSH", conv.Redis.ProcessingList, conv.Redis.WaitingList)
			if err != nil {
				c.Close()
				log.Error("", err)
				return err
			} else if s == nil {
				c.Close()
				// All done, processing is empty.
				return nil
			}
		}
		c.Close()
	}
}

func (conv *Conveyor) Flush() error {
	if db.Pool == nil {
		err := errors.New("Database not initialized")
		log.Error("", err)
		return err
	}
	c := db.Pool.Get()
	defer c.Close()

	c.Send("MULTI")
	c.Send("DEL", conv.Redis.WaitingList)
	c.Send("DEL", conv.Redis.ProcessingList)
	c.Send("DEL", conv.Scheduler.ScheduleList)
	_, err := redis.Values(c.Do("EXEC"))
	return err
}

func (conv *Conveyor) AddTask(target, payload string) (*Task, error) {
	if db.Pool == nil {
		err := errors.New("Database not initialized")
		log.Error("", err)
		return nil, err
	}
	c := db.Pool.Get()
	defer c.Close()

	ID, err := redis.Int(c.Do("INCR", conv.Redis.TaskIdKey))
	if err != nil {
		log.Error("", err)
		return nil, err
	}

	task := &Task{
		Object:  "task",
		ID:      ID,
		Target:  target,
		Payload: payload,
		Tries:   0,
		Delay:   0,
	}
	t, err := json.Marshal(task)
	if err != nil {
		log.Error("", err)
		return nil, err
	}
	_, err = c.Do("LPUSH", conv.Redis.WaitingList, t)
	if err != nil {
		log.Error("", err)
		return nil, err
	}
	return task, nil
}

func (conv *Conveyor) Stats() (*Statistic, error) {
	if db.Pool == nil {
		err := errors.New("Database not initialized")
		log.Error("", err)
		return nil, err
	}
	c := db.Pool.Get()
	defer c.Close()

	stats := &Statistic{
		Object: "statistic",
	}

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
	}

	return stats, nil
}

func (conv *Conveyor) removeProcessing(c *redis.Conn, data []byte, status string) error {
	_, err := redis.Int((*c).Do("LREM", conv.Redis.ProcessingList, "1", data))
	if err != nil {
		log.Error("", err)
		return err
	}

	switch status {
	case "ok":
		(*c).Do("INCR", conv.Redis.StatsTotalOK)
	case "error":
		(*c).Do("INCR", conv.Redis.StatsTotalError)
	}

	return nil
}

func (conv *Conveyor) Tasks() ([]Task, error) {
	if db.Pool == nil {
		return nil, errors.New("Database not initialized")
	}
	c := db.Pool.Get()
	defer c.Close()

	reply, err := redis.Values(c.Do("LRANGE", conv.Redis.WaitingList, "0", "-1"))
	if err != nil {
		log.Error("", err)
		return nil, err
	}

	// Make a new slice of equal length as result. Type assert to []byte and
	// JSON decode into slice element.
	resp := make([]Task, len(reply))
	for i, v := range reply {
		err = json.Unmarshal(v.([]byte), &resp[i])
		if err != nil {
			log.Error("", err)
			return nil, err
		}
	}
	return resp, nil
}
