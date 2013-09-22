package web

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"

	"github.com/borgenk/qdo/third_party/github.com/gorilla/mux"

	"github.com/borgenk/qdo/lib/db"
	"github.com/borgenk/qdo/lib/log"
	"github.com/borgenk/qdo/lib/queue"
)

type Header struct {
	Title string
}

type Page struct {
	Header Header
	Length int
}

var templates *template.Template

func renderTemplate(w http.ResponseWriter, tmpl string, p *Page) {
	err := templates.ExecuteTemplate(w, tmpl, p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func mainPage(w http.ResponseWriter, r *http.Request) {
	if db.Pool == nil {
		return
	}

	h := Header{
		Title: "QDo",
	}
	p := &Page{
		Header: h,
		Length: 10,
	}
	renderTemplate(w, "index", p)
}

// API handler for GET /api/conveyor.
func getAllConveyor(w http.ResponseWriter, r *http.Request) {
	res, err := queue.GetAllConveyor()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	ReturnJSONList(w, r, "/api/conveyor", len(res), res)
}

// API handler for POST /api/conveyor.
func createConveyor(w http.ResponseWriter, r *http.Request) {
	settings := &queue.Config{
		Name: r.FormValue("name"),
	}

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
	settings.NWorker = int32(v)

	v, err = strconv.Atoi(r.FormValue("throttle"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	settings.Throttle = int32(v)

	v, err = strconv.Atoi(r.FormValue("tasktlimit"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	settings.TaskTLimit = int32(v)

	v, err = strconv.Atoi(r.FormValue("taskmaxtries"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	settings.TaskMaxTries = int32(v)

	v, err = strconv.Atoi(r.FormValue("logsize"))
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	settings.LogSize = int32(v)

	err = queue.AddConveyor(settings)
	if err != nil {
		log.Error("", err)
		http.Error(w, "", http.StatusBadRequest)
		return
	}

	http.Redirect(w, r, "/", http.StatusFound)
}

func createTask(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_ = vars["id"]
}

func Run(port int, documentRoot string) {
	templates = template.Must(template.ParseFiles(documentRoot + "index.html"))
	r := mux.NewRouter()
	r.HandleFunc("/", mainPage).Methods("GET")
	r.HandleFunc("/api/conveyor", getAllConveyor).Methods("GET")
	r.HandleFunc("/api/conveyor", createConveyor).Methods("POST")
	r.HandleFunc("/api/conveyor/{id}/task", createTask).Methods("POST")

	http.Handle("/", r)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

type jsonResult struct {
	Object string      `json:"object"`
	URL    string      `json:"url"`
	Count  int         `json:"count"`
	Data   interface{} `json:"data"`
}

func ReturnJSONList(w http.ResponseWriter, r *http.Request, resource string,
	count int, data interface{}) {
	resp := jsonResult{
		Object: "list",
		URL:    resource,
		Count:  count,
		Data:   data,
	}

	pretty := r.FormValue("pretty")
	if pretty != "" {
		b, err := json.MarshalIndent(resp, "", " ")
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
