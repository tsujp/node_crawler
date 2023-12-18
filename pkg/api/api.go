package api

import (
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/ethereum/node-crawler/public"
)

type API struct {
	db                   *database.DB
	statsUpdateFrequency time.Duration
	enode                string
	snapshotDir          string

	stats      database.AllStats
	statsLock  sync.RWMutex
	statsCache map[string]CachedPage
}

func New(
	db *database.DB,
	statsUpdateFrequency time.Duration,
	enode string,
	snapshotDir string,
) *API {
	api := &API{
		db:                   db,
		statsUpdateFrequency: statsUpdateFrequency,
		enode:                enode,
		snapshotDir:          snapshotDir,

		stats:      database.AllStats{},
		statsLock:  sync.RWMutex{},
		statsCache: map[string]CachedPage{},
	}

	// go api.statsUpdaterDaemon()

	return api
}

type CachedPage struct {
	Page       []byte
	ValidUntil time.Time
}

func (a *API) replaceStats(newStats database.AllStats) {
	a.statsLock.Lock()
	defer a.statsLock.Unlock()

	a.stats = newStats
	a.statsCache = map[string]CachedPage{}
}

func (a *API) getStats() database.AllStats {
	a.statsLock.RLock()
	defer a.statsLock.RUnlock()

	return a.stats
}

func (a *API) getCache(params string) ([]byte, bool) {
	a.statsLock.RLock()
	defer a.statsLock.RUnlock()

	b, ok := a.statsCache[params]
	if !ok {
		return nil, false
	}

	if b.ValidUntil.Before(time.Now()) {
		return nil, false
	}

	return b.Page, true
}

func (a *API) setCache(params string, b []byte, validUntil time.Time) {
	a.statsLock.Lock()
	defer a.statsLock.Unlock()

	a.statsCache[params] = CachedPage{
		Page:       b,
		ValidUntil: validUntil,
	}
}

func NodeRoot(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("nodes ROOT"))
}

func NodeById(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("nodes BY ID"))
}

func SiteRoot(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("site ROOT"))
}

func (a *API) StartServer(wg *sync.WaitGroup, address string) {
	defer wg.Done()

	// It's nicer (for developers) to have all the routes in one place .'.
	//   being easier to reason about.

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)

	// TODO: Middleware to strip trailing slash off of URL.
	// TODO: Site name via config.

	// xx/nodes?page=N&synced=[yes|no]&network=NAME
	// xx/nodes/id
	r.Route("/", func(r chi.Router) {
		r.Get("/", a.handleRoot)
		r.Get("/favicon.ico", handleFavicon)
		serveEmbeddedFiles(r, "/static", public.StaticFiles)
	})

	r.Route("/nodes", func(r chi.Router) {
		r.Get("/", NodeRoot)
		r.Get("/{nodeId}", NodeById)
	})

	log.Info("Starting API", "address", address)
	http.ListenAndServe(address, r)

	// TODO: Old routes added back in (gradually as frontend developed).
	// router.HandleFunc("/", a.handleRoot)
	// router.HandleFunc("/favicon.ico", handleFavicon)
	// router.HandleFunc("/help/", a.handleHelp)
	// router.HandleFunc("/history/", a.handleHistoryList)
	// router.HandleFunc("/nodes/", a.nodesHandler)
	// router.HandleFunc("/snapshots/", a.handleSnapshots)
	// router.HandleFunc("/static/", handleStatic)

	// log.Info("Starting API", "address", address)
}
