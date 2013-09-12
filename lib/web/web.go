package web

import (
	"fmt"
	"html/template"
	"net/http"

	"github.com/borgenk/qdo/third_party/github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/lib/db"
	"github.com/borgenk/qdo/lib/queue"
	"github.com/borgenk/qdo/lib/log"
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

func handler(w http.ResponseWriter, r *http.Request) {
	c := db.Pool.Get()
	err := c.Err()
	if err != nil {
		log.Error("", err)
		return
	}
	defer c.Close()

	len, err := Length()

	h := Header{
		Title: "QDo",
	}
	p := &Page{
		Header: h,
		Length: len,
	}
	renderTemplate(w, "index", p)
}

func Run(port int, documentRoot string, dbc db.Config) {
	templates = template.Must(template.ParseFiles(documentRoot + "index.html"))

	http.HandleFunc("/", handler)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func Length() (int, error) {
	c := db.Pool.Get()
	err := c.Err()
	if err != nil {
		log.Error("", err)
		return 0, err
	}
	defer c.Close()
	return redis.Int(c.Do("LLEN", queue.WaitingList))
}
