package web

import (
	"html/template"
	"net/http"

	"github.com/garyburd/redigo/redis"

	"github.com/borgenk/qdo/db"
	"github.com/borgenk/qdo/log"
)

type Header struct {
	Title string
}

type Page struct {
	Header Header
	Length int
}

var templates = template.Must(template.ParseFiles("web/template/index.html"))

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

func Run(dbc db.Config) {
	http.HandleFunc("/", handler)
	http.ListenAndServe(":8080", nil)
}

func Length() (int, error) {
	c := db.Pool.Get()
	err := c.Err()
	if err != nil {
		log.Error("", err)
		return 0, err
	}
	defer c.Close()
	return redis.Int(c.Do("LLEN", db.WaitingList))
}
