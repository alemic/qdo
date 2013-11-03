package web

import (
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type fileHandler struct {
	root http.FileSystem
}

func FileServer(root http.FileSystem) http.Handler {
	return &fileHandler{root}
}

func (f *fileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upath := r.URL.Path
	if !strings.HasPrefix(upath, "/") {
		upath = "/" + upath
		r.URL.Path = upath
	}
	serveFile(w, r, f.root, path.Clean(upath))
}

func serveFile(w http.ResponseWriter, r *http.Request, fs http.FileSystem, name string) {
	content, err := fs.Open(name)
	if err != nil {
		http.Error(w, "", http.StatusNotFound)
		return
	}

	ctype := w.Header().Get("Content-Type")
	if ctype == "" {
		ctype = mime.TypeByExtension(filepath.Ext(name))
		if ctype == "" {
			// read a chunk to decide between utf-8 text and binary
			var buf [1024]byte
			n, _ := io.ReadFull(content, buf[:])
			b := buf[:n]
			ctype = http.DetectContentType(b)
			_, err := content.Seek(0, os.SEEK_SET) // rewind to output whole file
			if err != nil {
				http.Error(w, "seeker can't seek", http.StatusInternalServerError)
				return
			}
		}
		w.Header().Set("Content-Type", ctype)
	}

	dir := fmt.Sprintf("%s", fs)
	if dir == "" {
		dir = "."
	}
	b, err := ioutil.ReadFile(filepath.Join(dir, filepath.FromSlash(path.Clean("/"+name))))
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	w.Write(b)
}
