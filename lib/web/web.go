package web

import (
	"fmt"
	"html/template"
	"net/http"

	"github.com/borgenk/qdo/third_party/github.com/gorilla/mux"

	"github.com/borgenk/qdo/lib/db"
	_ "github.com/borgenk/qdo/lib/log"
	_ "github.com/borgenk/qdo/lib/queue"
)

type Header struct {
	Title string
}

type Page struct {
	Header Header
	Length int
}

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

func MainPage(w http.ResponseWriter, r *http.Request) {
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
	RenderTemplate(w, "index.html", p)
}

func NewConveyor(w http.ResponseWriter, r *http.Request) {
	h := Header{
		Title: "QDo",
	}
	p := &Page{
		Header: h,
	}
	RenderTemplate(w, "conveyor_form.html", p)
}

func CreateNewConveyor(w http.ResponseWriter, r *http.Request) {
	createConveyor(w, r)
}

func Run(port int, documentRoot string) {
	RegisterTemplate("index.html", template.Must(template.ParseFiles(
		documentRoot+"index.html", documentRoot+"layout.html")))

	RegisterTemplate("conveyor_form.html", template.Must(template.ParseFiles(
		documentRoot+"conveyor_form.html", documentRoot+"layout.html")))

	r := mux.NewRouter()
	r.HandleFunc("/", MainPage).Methods("GET")
	r.HandleFunc("/conveyor/new", NewConveyor).Methods("GET")
	r.HandleFunc("/conveyor/new", CreateNewConveyor).Methods("POST")

	// API Paths.
	r.HandleFunc("/api/conveyor", getAllConveyor).Methods("GET")
	r.HandleFunc("/api/conveyor", createConveyor).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}", getConveyor).Methods("GET")
	r.HandleFunc("/api/conveyor/{conveyor_id}", updateConveyor).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}/task", getAllTasks).Methods("GET")
	r.HandleFunc("/api/conveyor/{conveyor_id}/task", createTask).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}/stats", getStats).Methods("GET")

	http.Handle("/", r)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
