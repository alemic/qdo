package queue

import (
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/db"
	"github.com/borgenk/qdo/log"
)

type Config struct {
	NWorker      int32         // Number of simultaneous workers processing tasks.
	Throttle     time.Duration // Number of maxium task invocations from queue per second.
	TaskTLimit   time.Duration // Duration allowed per task to complete.
	TaskMaxTries int32         // Number of tries per task before giving up.
}

type Job struct {
	URL     string
	Payload []byte
	Tries   int32
	Delay   int32
}

func Run(dbc db.Config, qc Config) {
	db.ConnectPool(dbc)
	var c redis.Conn
	for {
		c = db.Pool.Get()
		err := c.Err()
		if err != nil {
			log.Error("", err)
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
	wk := make(chan int, qc.NWorker)

	for {
		select {
		case wk <- 1:
			b, err := redis.Bytes(c.Do("BRPOPLPUSH", db.WaitingList,
				db.ProcessingList, "0"))
			if err != nil {
				// No retry / reconnect logic yet.
				log.Error("", err)
				os.Exit(1)
			}

			go request(&qc, wk, b)

			// Throttle task invocations per second.
			time.Sleep(qc.Throttle)
		case <-si:
			log.Info("program killed")
			os.Exit(0)
		}

	}
}

// Move all entries in processing list to waiting queue.
func requeueAll(c *redis.Conn) error {
	for {
		s, err := (*c).Do("RPOPLPUSH", db.ProcessingList, db.WaitingList)
		if err != nil {
			log.Error("", err)
			return err
		}
		if s == nil {
			return nil
		}
	}
}

func scheduler() {
	c := db.Pool.Get()
	err := c.Err()
	if err != nil {
		log.Error("", err)
		return
	}
	defer c.Close()

	// Script arguments:
	//      Schdule list
	//      Timestamp now
	//      Waiting list
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
		_, err = rescheduleLua.Do(c, db.Schedulelist, now, db.WaitingList)
		if err != nil {
			log.Error("", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func removeProcessing(c *redis.Conn, j []byte) error {
	_, err := redis.Int((*c).Do("LREM", db.ProcessingList, "1", j))
	if err != nil {
		log.Error("", err)
		return err
	}
	return nil
}

func requeueJob(c *redis.Conn, job Job) error {
	j, err := json.Marshal(job)
	if err != nil {
		log.Error("", err)
		return err
	}
	_, err = (*c).Do("LPUSH", db.WaitingList, j)
	if err != nil {
		log.Error("", err)
		return err
	}
	return nil
}

func request(qc *Config, ch chan int, j []byte) {
	defer func() { <-ch }()

	c := db.Pool.Get()
	err := c.Err()
	if err != nil {
		log.Error("", err)
		return
	}
	defer c.Close()

	job := Job{}
	err = json.Unmarshal(j, &job)
	if err != nil {
		// Assume invalid job, discard it.
		log.Error("invalid task format", err)
		removeProcessing(&c, j)
		return
	}

	var p map[string]string
	err = json.Unmarshal(job.Payload, &p)
	if err != nil {
		log.Error("", err)
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
		log.Error("invalid URL, discarding job", err)
		removeProcessing(&c, j)
		return
	}

	log.Infof("processing new task: %s", job.URL)

	transport := http.Transport{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, (*qc).TaskTLimit)
		},
		Proxy: http.ProxyFromEnvironment,
		ResponseHeaderTimeout: (*qc).TaskTLimit,
	}
	client := http.Client{
		Transport: &transport,
	}
	resp, err := client.Post(job.URL, "application/x-www-form-urlencoded",
		strings.NewReader(values.Encode()))
	if err == nil {
		resp.Body.Close()
	}

	if err == nil && resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		removeProcessing(&c, j)
		log.Infof("task completed successfully: %s", resp.Status)
		return
	} else if err == nil && resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		removeProcessing(&c, j)
		log.Infof("task failed, request invalid: %s", resp.Status)
		return
	}

	if job.Tries >= (*qc).TaskMaxTries {
		// Remove it from ProcessingList
		log.Infof("task reached max tries: %d", job.Tries)
		removeProcessing(&c, j)
		// TODO: store/dump task somewhere?
		return
	}

	if job.Delay == 0 {
		job.Delay = 1
	}
	job.Delay = job.Delay * 2
	job.Tries = job.Tries + 1

	rj, err := json.Marshal(job)
	if err != nil {
		log.Error("", err)
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
	//      Schedule id
	//      Job bytes
	//      Schedule list
	//      Schedule timestamp
	//      Processing list
	//      Old job bytes
	var delayRetry = redis.NewScript(6,
		`local id = redis.call("INCR", KEYS[1])
         local job = id .. ":" .. KEYS[2]
         redis.call('ZADD', KEYS[3], KEYS[4], job)
         redis.call('LREM', KEYS[5], 1, KEYS[6])`)

	_, err = delayRetry.Do(c, db.ScheduleId, rj, db.Schedulelist, schedTs,
		db.ProcessingList, j)
	if err != nil {
		log.Infof("zombie left in processing: %s", err)
		return
	}

	log.Infof("task failed, rescheduled for retry in %d seconds", job.Delay)
}
