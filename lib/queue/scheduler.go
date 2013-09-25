package queue

import (
	"encoding/json"
	"time"

	"github.com/borgenk/qdo/third_party/github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/lib/db"
	"github.com/borgenk/qdo/lib/log"
)

const ScheduleId = "queue:scheduleid"
const ScheduleList = "queue:schedulelist"

type Scheduler struct {
	// Conveyor scheduler id name.
	ScheduleId string `json:"schedule_id"`

	// Conveyor scheduler list name.
	ScheduleList string `json:"schedule_list"`

	// How often scheduler checks schedule list in seconds.
	Rate time.Duration `json:"rate"`
}

func NewScheduler(conveyorID string) *Scheduler {
	scheduler := &Scheduler{
		ScheduleId:   "qdo:" + conveyorID + ":" + ScheduleId,
		ScheduleList: "qdo:" + conveyorID + ":" + ScheduleList,
		Rate:         5 * time.Second,
	}
	return scheduler
}

// Script arguments:
//   schedule list
//   current timestamp
//   queue list
const schedulerLua = `
local tasks = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", KEYS[2])
if tasks then
    for index = 1, #tasks do
        local j
        for i = 1, #tasks[index] do
            if tasks[index]:sub(i, i) == ":" then
                j = string.sub(tasks[index], i+1, -1)
                break
            end
        end
        local ok = redis.call("LPUSH", KEYS[3], j)
        if ok then redis.call("ZREM", KEYS[1], tasks[index]) end
    end
end`

func (sched *Scheduler) Start(queueList string) {
	var script = redis.NewScript(3, schedulerLua)
	for {
		c := db.Pool.Get()
		now := int32(time.Now().Unix())
		_, err := script.Do(c, sched.ScheduleList, now, queueList)
		c.Close()
		if err != nil {
			log.Error("", err)
		}
		time.Sleep(sched.Rate)
	}
}

func (sched *Scheduler) Stop() {

}

func (sched *Scheduler) Add() {

}

// Script arguments:
//   schedule id
//   job bytes
//   schedule list
//   schedule timestamp
//   processing list
//   old task bytes
const rescheduleLua = `
local id = redis.call("INCR", KEYS[1])
local task = id .. ":" .. KEYS[2]
redis.call('ZADD', KEYS[3], KEYS[4], task)
redis.call('LREM', KEYS[5], 1, KEYS[6])`

// Reschedule task.
// 1. Create new schedule id.
// 2. Add task to schdule list - format: <timestamp> - <id>:<task>.
// 3. Remove old task from processing list.
func (sched *Scheduler) Reschedule(queueList string, task *Task, oldTask []byte) (int32, error) {
	if task.Delay == 0 {
		task.Delay = 1
	}
	task.Delay = task.Delay * 2
	task.Tries = task.Tries + 1

	t, err := json.Marshal(task)
	if err != nil {
		log.Error("", err)
		return 0, err
	}
	retryAt := int32(time.Now().Unix()) + task.Delay

	var script = redis.NewScript(6, rescheduleLua)
	c := db.Pool.Get()
	_, err = script.Do(c, sched.ScheduleId, t, sched.ScheduleList, retryAt,
		queueList, oldTask)
	c.Close()
	if err != nil {
		log.Infof("zombie left in processing: %s", err)
		return 0, err
	}
	return task.Delay, nil
}
