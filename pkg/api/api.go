package api

import (
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/database"
)

type API struct {
	db                   *database.DB
	statsUpdateFrequency time.Duration
	enode                string
	snapshotDir          string

	stats      database.AllStats
	statsLock  sync.RWMutex
	statsCache map[string][]byte
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
		statsCache: map[string][]byte{},
	}

	go api.statsUpdaterDaemon()

	return api
}

func (a *API) replaceStats(newStats database.AllStats) {
	a.statsLock.Lock()
	defer a.statsLock.Unlock()

	a.stats = newStats
	a.statsCache = map[string][]byte{}
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

	return b, ok
}

func (a *API) setCache(params string, b []byte) {
	a.statsLock.Lock()
	defer a.statsLock.Unlock()

	a.statsCache[params] = b
}

func (a *API) StartServer(wg *sync.WaitGroup, address string) {
	defer wg.Done()

	router := http.NewServeMux()

	router.HandleFunc("/", a.handleRoot)
	router.HandleFunc("/favicon.ico", handleFavicon)
	router.HandleFunc("/help/", a.handleHelp)
	router.HandleFunc("/history/", a.handleHistoryList)
	router.HandleFunc("/nodes/", a.nodesHandler)
	router.HandleFunc("/snapshots/", a.handleSnapshots)
	router.HandleFunc("/static/", handleStatic)

	log.Info("Starting API", "address", address)
	_ = http.ListenAndServe(address, router)
}
