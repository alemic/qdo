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
	RegisterTemplate("index.html", template.Must(template.ParseFiles(
		documentRoot+"index.html", documentRoot+"layout.html")))

	RegisterTemplate("conveyor_form.html", template.Must(template.ParseFiles(
		documentRoot+"conveyor_form.html", documentRoot+"layout.html")))

	RegisterTemplate("conveyors.html", template.Must(template.ParseFiles(
		documentRoot+"conveyors.html", documentRoot+"layout.html")))

	RegisterTemplate("view_conveyor.html", template.Must(template.ParseFiles(
		documentRoot+"view_conveyor.html", documentRoot+"layout.html")))

	r := mux.NewRouter()
	r.HandleFunc("/", Conveyors).Methods("GET")
	r.HandleFunc("/admin/conveyors", Conveyors).Methods("GET")
	r.HandleFunc("/admin/conveyors/{conveyor_id}", adminViewConveyor).Methods("GET")

	r.HandleFunc("/conveyor/new", NewConveyor).Methods("GET")
	r.HandleFunc("/conveyor/new", CreateNewConveyor).Methods("POST")

	// API Paths.
	r.HandleFunc("/api/conveyor", getAllConveyor).Methods("GET")
	r.HandleFunc("/api/conveyor", createConveyor).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}", getConveyor).Methods("GET")
	r.HandleFunc("/api/conveyor/{conveyor_id}", updateConveyor).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}/task", getAllTasks).Methods("GET")
	r.HandleFunc("/api/conveyor/{conveyor_id}/task", createTask).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}/task", deleteAllTasks).Methods("DELETE")
	r.HandleFunc("/api/conveyor/{conveyor_id}/stats", getStats).Methods("GET")

	http.Handle("/", r)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
