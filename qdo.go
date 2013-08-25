package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/log"
)

const Version = 0.1

// Number of simultaneous workers processing tasks.
const nWorker = 5 // TODO: This should be configurable.

// Number of maxium task invocations from queue per second.
const throttle = (time.Second / 1) * time.Millisecond // TODO: This should be configurable.

// Duration allowed per task to complete.
const taskTLimit = time.Duration(10 * time.Minute) // TODO: This should be configurable.

// Number of tries per task before giving up.
const taskMaxTries = 10 // TODO: This should be configurable.

// Redis
const dbHost = "127.0.0.1:6379" // TODO: This should be configurable.
const dbPass = ""               // TODO: This should be configurable.
const dbIdx = "0"               // TODO: This should be configurable.
const waitingList = "waitinglist"
const processingList = "processinglist"
const scheduleList = "schedulelist"
const scheduleId = "scheduleid"

var DBPool *redis.Pool

var Log = log.New(os.Stdout, "", 0)

type Job struct {
	URL     string
	Payload []byte
	Tries   int32
	Delay   int32
}

func connectPool(host *string, port *int) {
	DBPool = &redis.Pool{
		MaxActive:   nWorker + 2,
		MaxIdle:     nWorker + 2,
		IdleTimeout: 0,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", *host, *port))
			if err != nil {
				Log.Error("", err)
				return nil, err
			}
			if dbPass != "" {
				_, err = c.Do("AUTH", dbPass)
				if err != nil {
					c.Close()
					Log.Error("", err)
					return nil, err
				}
			}
			_, err = c.Do("SELECT", dbIdx)
			if err != nil {
				Log.Error("", err)
				return nil, err
			}
			return c, err
		},
	}
}

func main() {
	host := flag.String("h", "127.0.0.1", "host..")
	port := flag.Int("p", 6379, "port..")
	flag.Parse()

	Log.Infof("starting QDo %.1f", Version)

	connectPool(host, port)
	var c redis.Conn
	for {
		c = DBPool.Get()
		err := c.Err()
		if err != nil {
			time.Sleep(5 * time.Second)
		} else {
			defer c.Close()
			break
		}
	}

	// Treat existing tasks in processing list as failed. Reschedule to waiting
	// queue.
	_ = requeueAll(&c)

	// Start scheduler for delayed tasks.
	go scheduler()

	si := make(chan os.Signal)
	signal.Notify(si, os.Interrupt)
	signal.Notify(si, os.Kill)

	// Limit number of simultaneous workers processing tasks.
	wk := make(chan int, nWorker)

	for {
		select {
		case wk <- 1:
			b, err := redis.Bytes(c.Do("BRPOPLPUSH", waitingList,
				processingList, "0"))
			if err != nil {
				// No retry / reconnect logic yet.
				Log.Error("", err)
				os.Exit(1)
			}

			go request(wk, b)

			// Throttle task invocations per second.
			// TODO: This should be configurable.
			time.Sleep(throttle)
		case <-si:
			Log.Info("program killed")
			os.Exit(0)
		}

	}
}

// Move all entries in processing list to waiting queue.
func requeueAll(c *redis.Conn) error {
	for {
		s, err := (*c).Do("RPOPLPUSH", processingList, waitingList)
		if err != nil {
			Log.Error("", err)
			return err
		}
		if s == nil {
			return nil
		}
	}
}

func scheduler() {
	c := DBPool.Get()
	err := c.Err()
	if err != nil {
		Log.Error("", err)
		return
	}
	defer c.Close()

	// Script arguments:
	//		Schdule list
	//		Timestamp now
	//		Waiting list
	var rescheduleLua = redis.NewScript(3,
		`local jobs = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2])
		 if jobs then
		 	for index = 1, #jobs do
		 		local j
		 		for i = 1, #jobs[index] do
		 			if jobs[index]:sub(i, i) == ":" then
		 				j = string.sub(jobs[index], i+1, -1)
		 				break
		 			end
		 		end
		 		local ok = redis.call("LPUSH", KEYS[3], j)
		 		if ok then redis.call("ZREM", KEYS[1], jobs[index]) end
			end
		 end`)

	for {
		now := int32(time.Now().Unix())
		_, err = rescheduleLua.Do(c, scheduleList, now, waitingList)
		if err != nil {
			Log.Error("", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func removeProcessing(c *redis.Conn, j []byte) error {
	_, err := redis.Int((*c).Do("LREM", processingList, "1", j))
	if err != nil {
		Log.Error("", err)
		return err
	}
	return nil
}

func requeueJob(c *redis.Conn, job Job) error {
	j, err := json.Marshal(job)
	if err != nil {
		Log.Error("", err)
		return err
	}
	_, err = (*c).Do("LPUSH", waitingList, j)
	if err != nil {
		Log.Error("", err)
		return err
	}
	return nil
}

func dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, taskTLimit)
}

func request(ch chan int, j []byte) {
	defer func() { <-ch }()

	c := DBPool.Get()
	err := c.Err()
	if err != nil {
		Log.Error("", err)
		return
	}
	defer c.Close()

	job := Job{}
	err = json.Unmarshal(j, &job)
	if err != nil {
		// Assume invalid job, discard it.
		Log.Error("invalid task format", err)
		removeProcessing(&c, j)
		return
	}

	var p map[string]string
	err = json.Unmarshal(job.Payload, &p)
	if err != nil {
		Log.Error("", err)
		removeProcessing(&c, j)
		return
	}

	values := make(url.Values)
	for i, val := range p {
		values.Set(i, val)
	}

	_, err = url.Parse(job.URL)
	if err != nil {
		// Assume invalid job, discard it.
		Log.Error("invalid URL, discarding job", err)
		removeProcessing(&c, j)
		return
	}

	Log.Infof("processing new task: %s", job.URL)

	transport := http.Transport{
		Dial:  dialTimeout,
		Proxy: http.ProxyFromEnvironment,
		ResponseHeaderTimeout: taskTLimit,
	}
	client := http.Client{
		Transport: &transport,
	}
	resp, err := client.Post(job.URL, "application/x-www-form-urlencoded", strings.NewReader(values.Encode()))
	if err == nil {
		resp.Body.Close()
	}

	if err == nil && resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		removeProcessing(&c, j)
		Log.Infof("task completed successfully: %s", resp.Status)
		return
	} else if err == nil && resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		removeProcessing(&c, j)
		Log.Infof("task failed, request invalid: %s", resp.Status)
		return
	}

	if job.Tries >= taskMaxTries {
		// Log error
		// Dump data to tmp file
		// Remove it from processinglist
		Log.Infof("task reached max tries: %d", job.Tries)
		removeProcessing(&c, j)
		return
	}

	if job.Delay == 0 {
		job.Delay = 1
	}
	job.Delay = job.Delay * 2
	job.Tries = job.Tries + 1

	rj, err := json.Marshal(job)
	if err != nil {
		Log.Error("", err)
		return
	}
	schedTs := int32(time.Now().Unix()) + job.Delay

	// Reschedule job.
	//
	// 1. Create new schedule id.
	// 2. Add job to schdule list - format: <timestamp> - <id>:<job>.
	// 3. Remove old job from processing list.
	//
	// Script arguments:
	// 		Schedule id
	// 		Job bytes
	// 		Schedule list
	//		Schedule timestamp
	//		Processing list
	// 		Old job bytes
	var delayRetry = redis.NewScript(6,
		`local id = redis.call("INCR", KEYS[1])
		 local job = id .. ":" .. KEYS[2]
		 redis.call('ZADD', KEYS[3], KEYS[4], job)
		 redis.call('LREM', KEYS[5], 1, KEYS[6])`)

	_, err = delayRetry.Do(c, scheduleId, rj, scheduleList, schedTs,
		processingList, j)
	if err != nil {
		Log.Infof("zombie left in processing: %s", err)
		return
	}

	Log.Infof("task failed, rescheduled for retry in %d seconds", job.Delay)
}
