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

const WaitingList = "waitinglist"
const ProcessingList = "processinglist"

type Conveyor struct {
	// Prefix all conveyor keys. Example site name.
	Prefix string `json: prefix`

	// Conveyor name.
	Name string `json: name`

	// Conveyor settings.
	Settings Config `json: config`

	// Limit number of simultaneous workers processing tasks.
	NotifyReady chan int `json: -`
}

type Config struct {
	// Number of simultaneous workers processing tasks.
	NWorker int32 `json: n_worker`

	// Number of maxium task invocations from queue per second.
	Throttle time.Duration `json: throttle`

	// Duration allowed per task to complete.
	TaskTLimit time.Duration `json: task_t_limit`

	// Number of tries per task before giving up.
	TaskMaxTries int32 `json: task_max_tries`
}

type Task struct {
	URL     string `json: url`
	Payload string `json: payload`
	Tries   int32  `json: tries`
	Delay   int32  `json: delay`
}

func StartConveyor(prefix string, name string, settings Config) error {
	conveyor := &Conveyor{
		Prefix:      prefix,
		Name:        name,
		Settings:    settings,
		NotifyReady: make(chan int, settings.NWorker),
	}
	return conveyor.Start()
}

func (conv *Conveyor) Start() error {
	// Treat existing tasks in processing list as failed. Reschedule to waiting
	// queue. Also makes sure we have database connection before we go any
	// further.
	err := conv.reset()
	if err != nil {
		return err
	}

	// Start scheduler for delayed tasks.
	// TODO: make this a struct. should be passed to request processer so it
	// cant add itself to rescheduling.
	go scheduler()

	for {
		conv.NotifyReady <- 1
		c := db.Pool.Get()
		b, err := redis.Bytes(c.Do("BRPOPLPUSH", WaitingList, ProcessingList, "0"))
		if err != nil {
			c.Close()
			log.Error("error while fetching new task", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		go conv.process(b)

		// Throttle task invocations per second.
		time.Sleep(conv.Settings.Throttle)
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
			return net.DialTimeout(network, addr, conv.Settings.TaskTLimit)
		},
		Proxy: http.ProxyFromEnvironment,
		ResponseHeaderTimeout: conv.Settings.TaskTLimit,
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

	if conv.Settings.TaskMaxTries > 0 && task.Tries >= conv.Settings.TaskMaxTries {
		// Remove from ProcessingList.
		log.Infof("task reached max tries: %d", task.Tries)
		conv.removeProcessing(&c, data)
		return
	}

	if task.Delay == 0 {
		task.Delay = 1
	}
	task.Delay = task.Delay * 2
	task.Tries = task.Tries + 1

	updatedTask, err := json.Marshal(task)
	if err != nil {
		log.Error("", err)
		return
	}
	schedTs := int32(time.Now().Unix()) + task.Delay

	// Reschedule task.
	//
	// 1. Create new schedule id.
	// 2. Add task to schdule list - format: <timestamp> - <id>:<task>.
	// 3. Remove old task from processing list.
	//
	// Script arguments:
	//      Schedule id
	//      Job bytes
	//      Schedule list
	//      Schedule timestamp
	//      Processing list
	//      Old task bytes
	var delayRetry = redis.NewScript(6,
		`local id = redis.call("INCR", KEYS[1])
         local task = id .. ":" .. KEYS[2]
         redis.call('ZADD', KEYS[3], KEYS[4], task)
         redis.call('LREM', KEYS[5], 1, KEYS[6])`)

	_, err = delayRetry.Do(c, ScheduleId, updatedTask, Schedulelist, schedTs,
		ProcessingList, data)
	if err != nil {
		log.Infof("zombie left in processing: %s", err)
		return
	}

	log.Infof("task failed, rescheduled for retry in %d seconds", task.Delay)
}

func (conv *Conveyor) reset() error {
	for {
		c := db.Pool.Get()
		err := c.Err()
		if err != nil {
			log.Error("", err)
			time.Sleep(5 * time.Second)
		} else {
			s, err := c.Do("RPOPLPUSH", ProcessingList, WaitingList)
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
	_, err := redis.Int((*c).Do("LREM", ProcessingList, "1", data))
	if err != nil {
		log.Error("", err)
		return err
	}
	return nil
}
