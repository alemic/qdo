package main

import (
	"encoding/json"
	"testing"

	"github.com/borgenk/qdo/lib/db"
	"github.com/borgenk/qdo/lib/queue"
)

type debugTask struct {
	DebugWait       string
	DebugHTTPStatus string
}

// Job 1
var j1Task = debugTask{
	DebugWait: "10",
}
var j1TaskJ, _ = json.Marshal(j1Task)
var j1 = queue.Task{
	Tries:   0,
	URL:     "http://10.0.2.15:8000/test",
	Payload: string(j1TaskJ),
	Delay:   0,
}

// Job 2
var j2Task = debugTask{
	DebugHTTPStatus: "500",
}
var j2TaskJ, _ = json.Marshal(j2Task)
var j2 = queue.Task{
	Tries:   0,
	URL:     "http://10.0.2.15:8000/test",
	Payload: string(j2TaskJ),
	Delay:   0,
}

// Job 3
var j3Task = debugTask{}
var j3TaskJ, _ = json.Marshal(j3Task)
var j3 = queue.Task{
	Tries:   0,
	URL:     "http://10.0.2.15:8000/test",
	Payload: string(j3TaskJ),
	Delay:   0,
}

func TestInsertJobs(t *testing.T) {
	dbc := db.Config{
		Host: dbDefaultHost,
		Port: dbDefaultPort,
		Pass: dbDefaultPass,
		Idx:  dbDefaultIdx,
	}
	db.ConnectPool(dbc)

	c := db.Pool.Get()
	err := c.Err()
	if err != nil {
		t.Fatalf("Command failed: %s", err.Error())
		return
	}
	defer c.Close()

	j, _ := json.Marshal(j1)
	_, err = c.Do("LPUSH", "dev:test:"+queue.WaitingList, j)
	if err != nil {
		t.Fatalf("Command failed: %s", err.Error())
		return
	}

	j, _ = json.Marshal(j2)
	_, err = c.Do("LPUSH", "dev:test:"+queue.WaitingList, j)
	if err != nil {
		t.Fatalf("Command failed: %s", err.Error())
		return
	}

	j, _ = json.Marshal(j3)
	for i := 0; i < 10; i++ {
		_, err = c.Do("LPUSH", "dev:test:"+queue.WaitingList, j)
		if err != nil {
			t.Fatalf("Command failed: %s", err.Error())
			return
		}
	}
}
