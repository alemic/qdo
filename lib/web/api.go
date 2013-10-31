package web

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/borgenk/qdo/third_party/github.com/gorilla/mux"

	"github.com/borgenk/qdo/lib/log"
	"github.com/borgenk/qdo/lib/queue"
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
	res, err := queue.GetAllConveyor()
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
	v, err = strconv.Atoi(r.FormValue("workers"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	config.NWorker = int32(v)

	v, err = strconv.Atoi(r.FormValue("throttle"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	config.Throttle = int32(v)

	v, err = strconv.Atoi(r.FormValue("task_t_limit"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	config.TaskTLimit = int32(v)

	v, err = strconv.Atoi(r.FormValue("task_max_tries"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	config.TaskMaxTries = int32(v)

	v, err = strconv.Atoi(r.FormValue("log_size"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	config.LogSize = int32(v)

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
	recurring := 0
	if r.FormValue("recurring") != "" {
		recurring, err = strconv.Atoi(r.FormValue("recurring"))
		if err != nil || recurring < 0 {
			http.Error(w, "value for recurring is invalid", http.StatusBadRequest)
			return
		}
	}

	res, err := conveyor.Add(r.FormValue("target"), r.FormValue("payload"),
		int64(scheduled), int64(recurring))
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

	res, err := conveyor.Stats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ReturnJSON(w, r, res)
}
