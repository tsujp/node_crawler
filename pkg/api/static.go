package api

import (
	"net/http"
	"embed"

	"github.com/go-chi/chi/v5"
	"github.com/ethereum/node-crawler/public"
)

func serveEmbeddedFiles(r chi.Router, pfx string, f embed.FS) {
	pathPrefix := pfx + "/"

	// TODO: More headers: `etag`, `expires`, `date`, and `last-modified`.
	// TODO: In general compression of responses for all routes, probably via custom chi middleware.
	// TODO: Asset pipeline (basic hash of filename for cache invalidation).
	// TODO: Does not handle abhorrent URLs like `/static/////style.css`, in that it will happily serve `style.css` in that scenario.
	r.Get(pathPrefix+"*", func(w http.ResponseWriter, r *http.Request) {
		abc := http.StripPrefix(pathPrefix, http.FileServer(http.FS(f)))
		w.Header().Set("Cache-Control", "public, max-age=2592000") // 30 day cache.
		abc.ServeHTTP(w, r)
	})
}

func handleFavicon(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write(public.Favicon)
}
