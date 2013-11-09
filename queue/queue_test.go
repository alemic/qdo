package queue_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/borgenk/qdo/log"
	"github.com/borgenk/qdo/queue"
)

var resultPortal chan string

type TestPayload struct {
	Value string
}

func testSetup() error {
	resultPortal = make(chan string)
	log.InitLog(log.New())

	go queue.StartManager("/tmp/qdotest")
	time.Sleep(time.Millisecond * 25)

	c := &queue.Config{
		MaxConcurrent: 5,
		MaxRate:       100,
		TaskTimeout:   1,
		TaskMaxTries:  1,
	}
	queue.AddConveyor("test", c)

	http.HandleFunc("/", handler)
	go http.ListenAndServe(":9999", nil)

	return nil
}

func handler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var t TestPayload
	err := decoder.Decode(&t)
	if err != nil {
		resultPortal <- ""
		return
	}
	resultPortal <- t.Value
}

func TestConveyorTaskProcess(t *testing.T) {
	err := testSetup()
	if err != nil {
		t.Error(err)
	}
	payload := TestPayload{Value: "12345"}
	p, err := json.Marshal(payload)
	if err != nil {
		t.Error(err)
	}
	conveyor, err := queue.GetConveyor("test")
	if err != nil {
		t.Error(err)
	}
	if conveyor == nil {
		t.Error(err)
	}
	conveyor.Add("http://localhost:9999", string(p), 0)
	result := <-resultPortal
	if result != "12345" {
		t.Errorf("Expected result %s, got %s", "12345", result)
	}
}
