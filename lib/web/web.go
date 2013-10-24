package web

import (
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"os"
	//"time"

	"github.com/borgenk/qdo/third_party/github.com/gorilla/mux"

	"github.com/borgenk/qdo/lib/log"
)

var templateList = []string{
	"layout.html",
	"view_conveyor_list.html",
	"create_conveyor.html",
	"view_conveyor.html",
}

var Templates = make(map[string]*template.Template)

func validateFilepath(filepath string) {
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		log.Error(filepath, errors.New("File not found"))
		os.Exit(1)
	}
}

func registerTemplate(name string, t *template.Template) {
	Templates[name] = t
}

func renderTemplate(w http.ResponseWriter, tmpl string, p interface{}) {
	err := Templates[tmpl].ExecuteTemplate(w, "layout", p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func Run(port int, documentRoot string) {
	for k, v := range templateList {
		validateFilepath(documentRoot + "template/" + v)
		if k == 0 {
			continue
		}
		registerTemplate(v, template.Must(template.ParseFiles(
			documentRoot+"template/"+v, documentRoot+"template/layout.html")))
	}

	r := mux.NewRouter()
	r.HandleFunc("/", viewAllConveyors).Methods("GET")
	r.HandleFunc("/conveyors", viewAllConveyors).Methods("GET")
	r.HandleFunc("/conveyors/{conveyor_id}", viewConveyor).Methods("GET")
	r.HandleFunc("/conveyor/new", newConveyor).Methods("GET")
	r.HandleFunc("/conveyor/new", newConveyorCreate).Methods("POST")

	// API Paths.
	r.HandleFunc("/api/conveyor", getAllConveyor).Methods("GET")
	r.HandleFunc("/api/conveyor", createConveyor).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}", getConveyor).Methods("GET")
	r.HandleFunc("/api/conveyor/{conveyor_id}", updateConveyor).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}", deleteConveyor).Methods("DELETE")
	r.HandleFunc("/api/conveyor/{conveyor_id}/task", getAllTasks).Methods("GET")
	r.HandleFunc("/api/conveyor/{conveyor_id}/task", createTask).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}/task", deleteAllTasks).Methods("DELETE")
	r.HandleFunc("/api/conveyor/{conveyor_id}/stats", getStats).Methods("GET")

	r.PathPrefix("/static/").Handler(
		http.StripPrefix("/static/", http.FileServer(http.Dir(documentRoot+"static/"))))

	http.Handle("/", r)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
