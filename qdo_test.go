package main

import (
	"encoding/json"
	"testing"
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
var j1 = Job{
	Tries:   0,
	URL:     "http://10.0.2.15:8080/test",
	Payload: j1TaskJ,
	Delay:   0,
}

// Job 2
var j2Task = debugTask{
	DebugHTTPStatus: "500",
}
var j2TaskJ, _ = json.Marshal(j2Task)
var j2 = Job{
	Tries:   0,
	URL:     "http://10.0.2.15:8080/test",
	Payload: j2TaskJ,
	Delay:   0,
}

func TestInsertJobs(t *testing.T) {
	host := "10.0.2.15"
	port := 6379
	connectPool(&host, &port)

	c := DBPool.Get()
	err := c.Err()
	if err != nil {
		t.Fatalf("Command failed: %s", err.Error())
		return
	}
	defer c.Close()

	j, _ := json.Marshal(j1)
	_, err = c.Do("LPUSH", waitingList, j)
	if err != nil {
		t.Fatalf("Command failed: %s", err.Error())
		return
	}
	j, _ = json.Marshal(j2)
	_, err = c.Do("LPUSH", waitingList, j)
	if err != nil {
		t.Fatalf("Command failed: %s", err.Error())
		return
	}
}
