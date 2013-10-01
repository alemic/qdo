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

const TaskIdKey = "queue:task:next_id"

const WaitingList = "queue:waitinglist"
const ProcessingList = "queue:processinglist"

const LogMessageList = "log"

const StatsInQueue = "stat:inqueue"
const StatsInProcessing = "stat:inprocessing"
const StatsTotal = "stat:total"
const StatsTotalError = "stat:totalerror"
const StatsTotalOK = "stat:totalok"
const StatsTotalRescheduled = "stat:rescheduled"
const StatsTotalTime = "stat:totaltime"
const StatsAvgTimeRecent = "stat:avgtimerecent"

type Conveyor struct {
	// Define resource.
	Object string `json:"object"`

	// Conveyor identification.
	ID string `json:"id"`

	// Conveyor configurations.
	Config Config `json:"config"`

	// Limit number of simultaneous workers processing tasks.
	NotifyReady chan int `json:"-"`

	// Conveyor state.
	Paused bool `json:"paused"`

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

	// Scheduler.
	Scheduler *Scheduler `json:"scheduler"`
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
	conv := &Conveyor{
		Object:                "conveyor",
		ID:                    conveyorID,
		Config:                *config,
		Paused:                false,
		TaskIdKey:             "qdo:" + conveyorID + ":" + TaskIdKey,
		WaitingList:           "qdo:" + conveyorID + ":" + WaitingList,
		ProcessingList:        "qdo:" + conveyorID + ":" + ProcessingList,
		StatsTotal:            "qdo:" + conveyorID + ":" + StatsTotal,
		StatsTotalOK:          "qdo:" + conveyorID + ":" + StatsTotalOK,
		StatsTotalRescheduled: "qdo:" + conveyorID + ":" + StatsTotalRescheduled,
		StatsTotalError:       "qdo:" + conveyorID + ":" + StatsTotalError,
		StatsTotalTime:        "qdo:" + conveyorID + ":" + StatsTotalTime,
		StatsAvgTimeRecent:    "qdo:" + conveyorID + ":" + StatsAvgTimeRecent,
		Scheduler:             NewScheduler(conveyorID),
	}
	return conv
}

func (conv *Conveyor) Start() error {
	log.Infof("starting conveyor \"%s\" with %d worker(s)", conv.ID,
		conv.Config.NWorker)

	conv.NotifyReady = make(chan int, conv.Config.NWorker)

	// Treat existing tasks in processing list as failed. Reschedule to waiting
	// queue. Also makes sure we have database connection before we go any
	// further.
	err := conv.reset()
	if err != nil {
		return err
	}

	// Start scheduler for delayed or rescheduled tasks.
	go conv.Scheduler.Start(conv.WaitingList)

	for {
		// Block until conveyor is ready to process next task.
		conv.NotifyReady <- 1

		if conv.Paused {
			time.Sleep(1 * time.Second)
			<-conv.NotifyReady
			continue
		}

		c := db.Pool.Get()
		b, err := redis.Bytes(c.Do("BRPOPLPUSH", conv.WaitingList, conv.ProcessingList, "0"))
		c.Close()
		if err != nil {
			log.Error("error while fetching new task", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		go conv.process(b)

		// Throttle task invocations per second.
		if conv.Config.Throttle > 0 {
			time.Sleep(time.Duration(time.Second / (time.Duration(conv.Config.Throttle) * time.Second)))
		}
	}
}

func (conv *Conveyor) Pause() {
	// TODO Mutex lock / sync change to redis
	conv.Paused = true
}

func (conv *Conveyor) Resume() {
	// TODO Mutex lock / sync change to redis
	conv.Paused = false
}

func (conv *Conveyor) process(data []byte) {
	defer func() { <-conv.NotifyReady }()

	startTime := time.Now()

	c := db.Pool.Get()
	defer c.Close()

	c.Do("INCR", conv.StatsTotal)

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
		c.Do("INCRBY", conv.StatsTotalTime, strconv.Itoa(int(endTime.Sub(startTime))))
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

	c.Do("INCR", conv.StatsTotalError)

	delay, err := conv.Scheduler.Reschedule(conv.ProcessingList, task, data)
	if err != nil {
		log.Info("task failed")
		return
	}

	log.Infof("task failed, rescheduled for retry in %d seconds", delay)
	c.Do("INCR", conv.StatsTotalRescheduled)
}

func (conv *Conveyor) reset() error {
	for {
		c := db.Pool.Get()
		err := c.Err()
		if err != nil {
			log.Error("", err)
			time.Sleep(5 * time.Second)
		} else {
			s, err := c.Do("RPOPLPUSH", conv.ProcessingList, conv.WaitingList)
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

func (conv *Conveyor) AddTask(target, payload string) (*Task, error) {
	if db.Pool == nil {
		err := errors.New("Database not initialized")
		log.Error("", err)
		return nil, err
	}
	c := db.Pool.Get()
	defer c.Close()

	ID, err := redis.Int(c.Do("INCR", conv.TaskIdKey))
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
	_, err = c.Do("LPUSH", conv.WaitingList, t)
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
	c.Send("LLEN", conv.WaitingList)
	c.Send("LLEN", conv.ProcessingList)
	c.Send("ZCARD", conv.Scheduler.ScheduleList)
	c.Send("GET", conv.StatsTotal)
	c.Send("GET", conv.StatsTotalOK)
	c.Send("GET", conv.StatsTotalRescheduled)
	c.Send("GET", conv.StatsTotalError)
	c.Send("GET", conv.StatsTotalTime)
	c.Send("GET", conv.StatsAvgTimeRecent)
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
	_, err := redis.Int((*c).Do("LREM", conv.ProcessingList, "1", data))
	if err != nil {
		log.Error("", err)
		return err
	}

	switch status {
	case "ok":
		(*c).Do("INCR", conv.StatsTotalOK)
	case "error":
		(*c).Do("INCR", conv.StatsTotalError)
	}

	return nil
}

func GetAllTasks(conveyorID string) ([]Task, error) {
	if db.Pool == nil {
		return nil, errors.New("Database not initialized")
	}
	c := db.Pool.Get()
	defer c.Close()

	queueList := "qdo:" + conveyorID + ":" + WaitingList
	reply, err := redis.Values(c.Do("LRANGE", queueList, "0", "-1"))
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
