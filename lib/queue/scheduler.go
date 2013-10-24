package queue

import (
	"encoding/json"
	"time"

	"github.com/borgenk/qdo/lib/log"
)

type Scheduler struct {
	// Conveyor ID.
	ConveyorID string `json:"conveyor_id"`

	// Conveyor status signal.
	notifySignal chan convSignal `json:"-"`

	// Conveyor scheduler id name.
	ScheduleId string `json:"schedule_id"`

	// Conveyor scheduler list name.
	ScheduleList string `json:"schedule_list"`

	// How often scheduler checks schedule list in seconds.
	Rate time.Duration `json:"rate"`
}

func NewScheduler(conveyorID string) *Scheduler {
	scheduler := &Scheduler{
		ConveyorID: conveyorID,
		Rate:       5 * time.Second,
	}
	return scheduler
}

func (sched *Scheduler) Start() {
	sched.notifySignal = make(chan convSignal)

	for {
		select {
		case sig := <-sched.notifySignal:
			if sig == stop {
				log.Infof("stopping scheduler for conveyor %s", sched.ConveyorID)
				return
			}
		default:
		}

		// Query all task which is less than now
		// append them to queue somehow..
		//now := int32(time.Now().Unix())

		time.Sleep(sched.Rate)
	}
}

func (sched *Scheduler) Stop() {

}

func (sched *Scheduler) Add() {

}

// Reschedule task.
// 1. Create new schedule id.
// 2. Add task to schdule list - format: <timestamp> - <id>:<task>.
// 3. Remove old task from processing list.
func (sched *Scheduler) Reschedule(task *Task) (int32, error) {
	if task.Delay == 0 {
		task.Delay = 1
	}
	task.Delay = task.Delay * 2
	task.Tries = task.Tries + 1

	_, err := json.Marshal(task)
	// t
	if err != nil {
		log.Error("", err)
		return 0, err
	}
	//retryAt := int32(time.Now().Unix()) + task.Delay

	// append t to scheduler list.

	return task.Delay, nil
}
