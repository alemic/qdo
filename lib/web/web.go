package web

import (
	"fmt"
	"html/template"
	"net/http"

	"github.com/borgenk/qdo/third_party/github.com/gorilla/mux"
)

var Templates = make(map[string]*template.Template)

func RegisterTemplate(name string, t *template.Template) {
	Templates[name] = t
}

func RenderTemplate(w http.ResponseWriter, tmpl string, p interface{}) {
	err := Templates[tmpl].ExecuteTemplate(w, "layout", p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func Run(port int, documentRoot string) {
	RegisterTemplate("view_conveyor_list.html", template.Must(template.ParseFiles(
		documentRoot+"view_conveyor_list.html", documentRoot+"layout.html")))

	RegisterTemplate("create_conveyor.html", template.Must(template.ParseFiles(
		documentRoot+"create_conveyor.html", documentRoot+"layout.html")))

	RegisterTemplate("view_conveyor.html", template.Must(template.ParseFiles(
		documentRoot+"view_conveyor.html", documentRoot+"layout.html")))

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
