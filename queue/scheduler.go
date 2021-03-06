package queue

import (
	"bytes"
	"fmt"
	"time"

	"github.com/borgenk/qdo/third_party/github.com/syndtr/goleveldb/leveldb/comparer"

	"github.com/borgenk/qdo/log"
)

type Scheduler struct {
	Conveyor     *Conveyor       `json:"-"`             // Conveyor.
	notifySignal chan convSignal `json:"-"`             // Conveyor status signal.
	ScheduleId   string          `json:"schedule_id"`   // Conveyor scheduler id name.
	ScheduleList string          `json:"schedule_list"` // Conveyor scheduler list name.
	Rate         time.Duration   `json:"rate"`          // How often scheduler checks schedule list in seconds.
}

func NewScheduler(conveyor *Conveyor) *Scheduler {
	scheduler := &Scheduler{
		Conveyor: conveyor,
		Rate:     1 * time.Second,
	}
	return scheduler
}

func (sched *Scheduler) Start() {
	sched.notifySignal = make(chan convSignal)

	for {
		select {
		case sig := <-sched.notifySignal:
			if sig == stop {
				log.Infof("conveyor %s stopping scheduler", sched.Conveyor.ID)
				return
			}
		default:
		}

		// Fetch all tasks scheduled earlier then right now, starting with the
		// oldest (lowest) possible item.
		stop := append(sched.Conveyor.waitKeyStart, []byte(fmt.Sprintf("%d", time.Now().Unix()))...)
		iter := db.NewIterator(nil)
		for iter.Seek(sched.Conveyor.waitKeyStart); iter.Valid(); iter.Next() {
			k := iter.Key()
			v := iter.Value()

			if comparer.DefaultComparer.Compare(k, stop) > 0 {
				// This might be a task is sechduled in the future or some other
				// stored value. All scheduled tasks up until right now is read.
				break
			}

			// Parse out task id in order to avoid decode/encode gob. The task
			// id is in the last part of the key, seperated with \x00.
			i := bytes.LastIndex(k, []byte(startPoint))
			taskId := fmt.Sprintf("%s", (k[i+1 : len(k)]))
			log.Infof("conveyor %s task %s placed back into queue by scheduler",
				sched.Conveyor.ID, taskId)

			// TODO: Batch add / removal of task.
			sched.Conveyor.add(taskId, v)
			_ = db.Delete(k, nil) // TODO: Check error value..
			sched.Conveyor.Stats.InScheduled.Add(-1)
		}
		iter.Release()
		time.Sleep(sched.Rate)
	}
}

// Reschedule task.
func (sched *Scheduler) Reschedule(task *Task) (int32, error) {
	if task.Delay == 0 {
		task.Delay = 1
	}
	task.Delay = task.Delay * 2
	task.Tries = task.Tries + 1

	t, err := GobEncode(task)
	if err != nil {
		log.Error("", err)
		return 0, err
	}

	retryAt := time.Now().Unix() + int64(task.Delay)
	err = sched.Add(task.ID, t, retryAt)
	return task.Delay, err
}

func (sched *Scheduler) Add(taskId string, task []byte, time int64) error {
	// Key format: [conv id] \x00 [key type] \x00 [timestamp] \x00 [task id]
	key := append(sched.Conveyor.waitKeyStart,
		[]byte(fmt.Sprintf("%d%s%s", time, startPoint, taskId))...)
	err = db.Put(key, task, nil)
	if err != nil {
		log.Error("add task to db failed", err)
		return err
	}
	sched.Conveyor.Stats.InScheduled.Add(1)
	return nil
}
