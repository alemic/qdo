package web

import (
	"fmt"
	"html/template"
	"net/http"
	"strconv"

	_ "github.com/borgenk/qdo/third_party/github.com/garyburd/redigo/redis"
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

	//c := db.Pool.Get()
	//defer c.Close()

	h := Header{
		Title: "QDo",
	}
	p := &Page{
		Header: h,
		Length: 10,
	}
	renderTemplate(w, "index", p)
}

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
	r.HandleFunc("/api/conveyor", createConveyor).Methods("POST")
	r.HandleFunc("/api/conveyor/{id}/task", createTask).Methods("POST")

	http.Handle("/", r)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
