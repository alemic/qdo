package web

import (
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

type ConveyorResult struct {
	ID              string
	Status          string
	MaxRate         int32
	MaxConcurrent   int32
	TasksWaiting    int64
	TasksProcessing int64
}

type Result struct {
	List []*ConveyorResult
}

func viewAllConveyors(w http.ResponseWriter, r *http.Request) {
	res := make([]*ConveyorResult, 0)
	convs := queue.GetAllConveyor()
	for _, v := range convs {
		conv := &ConveyorResult{
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
		Title: "QDo",
	}
	p := &Page{
		Header: h,
		Title:  "Conveyors",
		Result: res,
	}
	RenderTemplate(w, "view_conveyor_list.html", p)
}

func viewConveyor(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["conveyor_id"]
	res, err := queue.GetConveyor(id)
	if err != nil {
		return
	}
	if res == nil {
		return
	}

	h := Header{
		Title: "QDo",
	}
	p := &Page{
		Header: h,
		Result: res,
	}
	RenderTemplate(w, "view_conveyor.html", p)
}

func newConveyor(w http.ResponseWriter, r *http.Request) {
	h := Header{
		Title: "QDo",
	}
	p := &Page{
		Header: h,
	}
	RenderTemplate(w, "create_conveyor.html", p)
}

func newConveyorCreate(w http.ResponseWriter, r *http.Request) {
	createConveyor(w, r)
}
