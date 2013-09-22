package queue

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/borgenk/qdo/third_party/github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/lib/db"
	"github.com/borgenk/qdo/lib/log"
)

const WaitingList = "queue:waitinglist"
const ProcessingList = "queue:processinglist"

const LogMessageList = "log"
const StatsTotal = "stat:total"
const StatsTotalError = "stat:totalerror"
const StatsTotalOk = "stat:totalok"
const StatsAvgTime = "stat:avgtime"
const StatsAvgTimeRecent = "stat:avgtimerecent"

type Conveyor struct {
	// Define resource.
	Object string `json:"object`

	// Prefix all conveyor keys. Example site name.
	Prefix string `json:"prefix"`

	// Conveyor identification.
	ID string `json:"id"`

	// Conveyor configurations.
	Config Config `json:"config"`

	// Limit number of simultaneous workers processing tasks.
	NotifyReady chan int `json:"-"`

	// Conveyor waiting list name.
	WaitingList string `json:"waiting_list"`

	// Conveyor processing list name.
	ProcessingList string `json:"processing_list"`

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

type Task struct {
	URL     string `json:"url"`
	Payload string `json:"payload"`
	Tries   int32  `json:"tries"`
	Delay   int32  `json:"delay"`
}

func NewConveyor(prefix, id string, config *Config) *Conveyor {
	conv := &Conveyor{
		Object:         "Conveyor",
		Prefix:         prefix,
		ID:             id,
		Config:         *config,
		WaitingList:    prefix + ":" + id + ":" + WaitingList,
		ProcessingList: prefix + ":" + id + ":" + ProcessingList,
		Scheduler:      NewScheduler(prefix, id),
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
		conv.NotifyReady <- 1
		c := db.Pool.Get()
		b, err := redis.Bytes(c.Do("BRPOPLPUSH", conv.WaitingList, conv.ProcessingList, "0"))
		if err != nil {
			c.Close()
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

func (conv *Conveyor) process(data []byte) {
	defer func() { <-conv.NotifyReady }()

	c := db.Pool.Get()
	defer c.Close()

	task := &Task{}
	err := json.Unmarshal(data, task)
	if err != nil {
		// Assume invalid job, discard it.
		log.Error("invalid task format", err)
		conv.removeProcessing(&c, data)
		return
	}

	_, err = url.Parse(task.URL)
	if err != nil {
		// Assume invalid job, discard it.
		log.Error("invalid URL, discarding job", err)
		conv.removeProcessing(&c, data)
		return
	}

	log.Infof("processing new task: %s", task.URL)

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
	resp, err := client.Post(task.URL, "application/json",
		bytes.NewReader([]byte(task.Payload)))
	if err == nil {
		resp.Body.Close()
	}

	if err == nil && resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		conv.removeProcessing(&c, data)
		log.Infof("task completed successfully: %s", resp.Status)
		return
	} else if err == nil && resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		conv.removeProcessing(&c, data)
		log.Infof("task failed, request invalid: %s", resp.Status)
		return
	}

	if conv.Config.TaskMaxTries > 0 && task.Tries >= conv.Config.TaskMaxTries {
		// Remove from ProcessingList.
		log.Infof("task reached max tries: %d", task.Tries)
		conv.removeProcessing(&c, data)
		return
	}

	delay, err := conv.Scheduler.Reschedule(conv.ProcessingList, task, data)
	if err != nil {
		log.Info("task failed")
	}
	log.Infof("task failed, rescheduled for retry in %d seconds", delay)
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
				log.Error("", err)
				return err
			} else if s == nil {
				// All done, processing is empty.
				return nil
			}
		}
		c.Close()
	}
}

func (conv *Conveyor) removeProcessing(c *redis.Conn, data []byte) error {
	_, err := redis.Int((*c).Do("LREM", conv.ProcessingList, "1", data))
	if err != nil {
		log.Error("", err)
		return err
	}
	return nil
}
