package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/ethereum/node-crawler/public"
)

func handleFavicon(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write(public.Favicon)
}

func handleStatic(w http.ResponseWriter, r *http.Request) {
	filename := strings.TrimPrefix(r.URL.Path, "/static/")
	file, ok := public.StaticFiles[filename]

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, "Not Found")

		return
	}

	w.Header().Set("Cache-Control", "public, max-age=31536000")
	_, _ = w.Write(file)
}
