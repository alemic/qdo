package web

import (
	"fmt"
	"net/http"

	"github.com/borgenk/qdo/third_party/github.com/gorilla/mux"

	"github.com/borgenk/qdo/lib/queue"
)

type Header struct {
	Title string
}

type Page struct {
	Header Header
	Title  string
	Result interface{}
}

type ConveyorRowResult struct {
	ID              string
	Status          string
	MaxRate         int32
	MaxConcurrent   int32
	TasksWaiting    int64
	TasksProcessing int64
}

func viewAllConveyors(w http.ResponseWriter, r *http.Request) {
	res := make([]*ConveyorRowResult, 0)
	convs := queue.GetAllConveyor()
	for _, v := range convs {
		conv := &ConveyorRowResult{
			ID:              v.ID,
			Status:          "Active",
			MaxRate:         v.Config.Throttle,
			MaxConcurrent:   v.Config.NWorker,
			TasksWaiting:    0,
			TasksProcessing: 0,
		}
		if v.Paused {
			conv.Status = "Paused"
		}
		stats, err := v.Stats()
		if err == nil {
			conv.TasksWaiting = stats.InQueue
			conv.TasksProcessing = stats.InProcessing
		}
		res = append(res, conv)
	}

	h := Header{
		Title: "Conveyors | QDo",
	}
	p := &Page{
		Header: h,
		Title:  "Conveyors",
		Result: res,
	}
	renderTemplate(w, "view_conveyor_list.html", p)
}

type ConveyorResult struct {
	Conv  *queue.Conveyor
	Stats *queue.Statistic
	Tasks []*queue.Task
}

func viewConveyor(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["conveyor_id"]
	conv, err := queue.GetConveyor(id)
	if err != nil {
		return
	}
	if conv == nil {
		return
	}
	convRes := &ConveyorResult{
		Conv: conv,
	}
	stats, err := conv.Stats()
	if err != nil {
		return
	}
	convRes.Stats = stats

	convRes.Tasks = make([]*queue.Task, 0)

	h := Header{
		Title: fmt.Sprintf("Conveyor %s | QDo", conv.ID),
	}
	p := &Page{
		Header: h,
		Title:  conv.ID,
		Result: convRes,
	}
	renderTemplate(w, "view_conveyor.html", p)
}

func newConveyor(w http.ResponseWriter, r *http.Request) {
	h := Header{
		Title: "QDo",
	}
	p := &Page{
		Header: h,
	}
	renderTemplate(w, "create_conveyor.html", p)
}

func newConveyorCreate(w http.ResponseWriter, r *http.Request) {
	createConveyor(w, r)
}
