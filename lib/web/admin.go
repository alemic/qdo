package web

import (
	"net/http"

	"github.com/borgenk/qdo/third_party/github.com/gorilla/mux"

	"github.com/borgenk/qdo/lib/db"
	_ "github.com/borgenk/qdo/lib/log"

	"github.com/borgenk/qdo/lib/queue"
)

type Header struct {
	Title string
}

type Page struct {
	Header Header
	Result interface{}
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

type Result struct {
	List []*queue.Conveyor
}

func Conveyors(w http.ResponseWriter, r *http.Request) {
	res := queue.GetAllConveyor()

	h := Header{
		Title: "QDo",
	}
	p := &Page{
		Header: h,
		Result: res,
	}
	RenderTemplate(w, "conveyors.html", p)
}

func adminViewConveyor(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["conveyor_id"]
	res, err := queue.GetConveyor(id)
	if err != nil {
		// error template
		return
	}
	if res == nil {
		// not found template
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
