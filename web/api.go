package web

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/borgenk/qdo/third_party/github.com/gorilla/mux"

	"github.com/borgenk/qdo/log"
	"github.com/borgenk/qdo/queue"
)

type jsonListResult struct {
	Object string      `json:"object"`
	URL    string      `json:"url"`
	Count  int         `json:"count"`
	Data   interface{} `json:"data"`
}

func JSONListResult(url string, count int, data interface{}) *jsonListResult {
	res := &jsonListResult{
		Object: "list",
		URL:    url,
		Count:  count,
		Data:   data,
	}
	return res
}

func ReturnJSON(w http.ResponseWriter, r *http.Request, resp interface{}) {
	if resp == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	pretty := r.FormValue("pretty")
	if pretty != "" {
		b, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
		w.Write(b)
	} else {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}
}

// API handler for GET /api/conveyor.
func getAllConveyor(w http.ResponseWriter, r *http.Request) {
	res, err := queue.GetAllConveyors()
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	ReturnJSON(w, r, JSONListResult("/api/conveyor", len(res), res))
}

// API handler for POST /api/conveyor.
func createConveyor(w http.ResponseWriter, r *http.Request) {
	conveyorID := r.FormValue("conveyor_id")
	config := &queue.Config{}
	var (
		v   int
		err error
	)
	v, err = strconv.Atoi(r.FormValue("max_concurrent"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	config.MaxConcurrent = int32(v)
	v, err = strconv.Atoi(r.FormValue("max_rate"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	config.MaxRate = int32(v)
	v, err = strconv.Atoi(r.FormValue("task_timeout"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	config.TaskTimeout = int32(v)
	v, err = strconv.Atoi(r.FormValue("task_max_tries"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	config.TaskMaxTries = int32(v)
	err = queue.AddConveyor(conveyorID, config)
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
}

// API handler for GET /api/conveyor/{conveyor_id}.
func getConveyor(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["conveyor_id"]
	res, err := queue.GetConveyor(id)
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	if res == nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	ReturnJSON(w, r, res)
}

// API handler for POST /api/conveyor/{conveyor_id}.
func updateConveyor(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["conveyor_id"]
	conv, err := queue.GetConveyor(id)
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	if conv == nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	paused := r.FormValue("paused")
	if paused == "true" {
		conv.Pause()
	} else if paused == "false" {
		conv.Resume()
	}
	ReturnJSON(w, r, nil)
}

func deleteConveyor(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["conveyor_id"]
	err := queue.RemoveConveyor(id)
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	ReturnJSON(w, r, nil)
}

// API handler for GET /api/conveyor/{conveyor_id}/task
func getAllTasks(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["conveyor_id"]
	conv, err := queue.GetConveyor(id)
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	if conv == nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	res, err := conv.Tasks()
	if err != nil {
		http.Error(w, "could not fetch tasks", http.StatusInternalServerError)
		return
	}
	ReturnJSON(w, r, JSONListResult("/api/conveyor/"+id+"/task", len(res), res))
}

// API handler for POST /api/conveyor/{conveyor_id}/task.
func createTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	conveyorID := vars["conveyor_id"]

	conveyor, err := queue.GetConveyor(conveyorID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if conveyor == nil {
		http.Error(w, "conveyor id does not exsist", http.StatusBadRequest)
		return
	}
	scheduled := 0
	if r.FormValue("scheduled") != "" {
		scheduled, err = strconv.Atoi(r.FormValue("scheduled"))
		if err != nil || scheduled < 0 {
			http.Error(w, "value for scheduled is invalid", http.StatusBadRequest)
			return
		}
	}

	res, err := conveyor.Add(r.FormValue("target"), r.FormValue("payload"),
		int64(scheduled))
	if err != nil {
		http.Error(w, "could not add task to database", http.StatusInternalServerError)
		return
	}
	ReturnJSON(w, r, res)
}

// API handler for DELETE /api/conveyor/{conveyor_id}/task.
func deleteAllTasks(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	conveyorID := vars["conveyor_id"]

	conveyor, err := queue.GetConveyor(conveyorID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if conveyor == nil {
		http.Error(w, "conveyor id does not exsist", http.StatusBadRequest)
		return
	}
	err = conveyor.Flush()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ReturnJSON(w, r, nil)
}

type StatsResponse struct {
	Object                    string `json:"object"`
	InQueue                   int64  `json:"in_queue"`
	InProcessing              int64  `json:"in_processing"`
	InScheduled               int64  `json:"in_scheduled"`
	TotalProcessed            int64  `json:"total_processed"`
	TotalProcessedOK          int64  `json:"total_processed_ok"`
	TotalProcessedError       int64  `json:"total_processed_error"`
	TotalProcessedRescheduled int64  `json:"total_processed_rescheduled"`
	TotalTime                 int64  `json:"total_time"`
	TimeLastOK                int64  `json:"time_last_ok"`
}

func (s *StatsResponse) Get(conveyor *queue.Conveyor) {
	s.Object = "statistic"
	s.InQueue = conveyor.Stats.InQueue.Get()
	s.InProcessing = conveyor.Stats.InProcessing.Get()
	s.InScheduled = conveyor.Stats.InScheduled.Get()
	s.TotalProcessed = conveyor.Stats.TotalProcessed.Get()
	s.TotalProcessedOK = conveyor.Stats.TotalProcessedOK.Get()
	s.TotalProcessedError = conveyor.Stats.TotalProcessedError.Get()
	s.TotalProcessedRescheduled = conveyor.Stats.TotalProcessedRescheduled.Get()
	s.TotalTime = conveyor.Stats.TotalTime.Get()
	s.TimeLastOK = conveyor.Stats.TimeLastOK.Get()
}

// API handler for GET /api/conveyor/{conveyor_id}/stats
func getStats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	conveyorID := vars["conveyor_id"]

	conveyor, err := queue.GetConveyor(conveyorID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if conveyor == nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	statsResp := &StatsResponse{}
	statsResp.Get(conveyor)
	ReturnJSON(w, r, statsResp)
}
