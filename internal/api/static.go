package api

import (
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"
)

// staticHandler serves the console build from disk (D1028). A path under /api/ is not a file. Anything else
// that is not a regular file inside the root — missing, a directory, or outside the root — is index.html,
// so the client-side router can take the URL. The root is an os.Root, so a path cannot leave the directory.
type staticHandler struct {
	root *os.Root
}

func (h staticHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if apiPath(r.URL.Path) {
		http.NotFound(w, r)
		return
	}
	name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if name != "" && name != "." {
		f, info, err := openRegular(h.root, name)
		if err == nil {
			defer f.Close()
			http.ServeContent(w, r, info.Name(), info.ModTime(), f)
			return
		}
		if !os.IsNotExist(err) {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
	}
	serveIndex(w, r, h.root)
}

func apiPath(p string) bool {
	p = path.Clean("/" + p)
	return p == "/api" || strings.HasPrefix(p, "/api/")
}

func openRegular(root *os.Root, name string) (*os.File, fs.FileInfo, error) {
	f, err := root.Open(name)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, nil, os.ErrNotExist
	}
	return f, info, nil
}

func serveIndex(w http.ResponseWriter, r *http.Request, root *os.Root) {
	f, info, err := openRegular(root, "index.html")
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	http.ServeContent(w, r, "index.html", info.ModTime(), f)
}
