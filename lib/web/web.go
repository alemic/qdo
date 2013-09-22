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

var templates *template.Template

func renderTemplate(w http.ResponseWriter, tmpl string, p *Page) {
	err := templates.ExecuteTemplate(w, tmpl, p)
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
	renderTemplate(w, "index", p)
}

func CreateNewConveyor(w http.ResponseWriter, r *http.Request) {
	createConveyor(w, r)
	http.Redirect(w, r, "/", http.StatusFound)
}

func Run(port int, documentRoot string) {
	templates = template.Must(template.ParseFiles(documentRoot + "index.html"))
	r := mux.NewRouter()
	r.HandleFunc("/", MainPage).Methods("GET")
	r.HandleFunc("/conveyor/new", CreateNewConveyor).Methods("POST")

	// API Paths.
	r.HandleFunc("/api/conveyor", getAllConveyor).Methods("GET")
	r.HandleFunc("/api/conveyor", createConveyor).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}", getConveyor).Methods("GET")
	r.HandleFunc("/api/conveyor/{conveyor_id}/task", getAllTasks).Methods("GET")
	r.HandleFunc("/api/conveyor/{conveyor_id}/task", createTask).Methods("POST")
	r.HandleFunc("/api/conveyor/{conveyor_id}/stats", getStats).Methods("GET")

	http.Handle("/", r)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
