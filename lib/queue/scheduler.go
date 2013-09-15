package queue

import (
	"time"

	"github.com/borgenk/qdo/third_party/github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/lib/db"
	"github.com/borgenk/qdo/lib/log"
)

const ScheduleId = "scheduleid"
const Schedulelist = "schedulelist"

func scheduler() {
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
		c := db.Pool.Get()
		err := c.Err()
		if err != nil {
			log.Error("", err)
			continue
		}

		now := int32(time.Now().Unix())
		_, err = rescheduleLua.Do(c, Schedulelist, now, WaitingList)
		c.Close()
		if err != nil {
			log.Error("", err)
		}
		time.Sleep(5 * time.Second)
	}
}
