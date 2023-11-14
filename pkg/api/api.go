package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/public"
)

type API struct {
	db                   *database.DB
	statsUpdateFrequency time.Duration
	enode                string

	stats     database.AllStats
	statsLock sync.Mutex
}

func New(db *database.DB, statsUpdateFrequency time.Duration, enode string) *API {
	api := &API{
		db:                   db,
		statsUpdateFrequency: statsUpdateFrequency,
		enode:                enode,

		stats: database.AllStats{},
	}

	go api.statsUpdaterDaemon()

	return api
}

func (a *API) replaceStats(newStats database.AllStats) {
	a.statsLock.Lock()
	defer a.statsLock.Unlock()

	a.stats = newStats
}

func (a *API) getStats() database.AllStats {
	a.statsLock.Lock()
	defer a.statsLock.Unlock()

	return a.stats
}

// Meant to be run as a goroutine
func (a *API) statsUpdaterDaemon() {
	for {
		start := time.Now()
		nextLoop := start.Truncate(a.statsUpdateFrequency).Add(a.statsUpdateFrequency)

		log.Debug("updating stats...")

		stats, err := a.db.GetStats(context.Background())
		if err != nil {
			log.Error("stats updater daemon: get stats failed", "err", err)
			time.Sleep(time.Minute)

			continue
		}

		a.replaceStats(stats)

		log.Debug("stats updated", "next", nextLoop, "took", time.Since(start))

		time.Sleep(time.Until(nextLoop))
	}
}

func mapPosition(x, y int) string {
	return fmt.Sprintf(
		`style="position: absolute; top: %d%%; left: %d%%; transform: translate(-50%%, -50%%)"`,
		y,
		x,
	)
}

func (a *API) nodesHandler(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")

	// /nodes/{id}
	if len(pathParts) != 3 {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	nodeID := pathParts[2]

	// /nodes/
	if nodeID == "" {
		a.nodesListHandler(w, r)

		return
	}

	nodes, err := a.db.GetNodeTable(r.Context(), nodeID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNotFound)

			return
		}
		log.Error("get node page failed", "err", err, "id", nodeID)
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	sb := new(strings.Builder)

	nodeTable := public.NodeTable(*nodes)
	index := public.Index(nodeTable, 1, -1)
	_ = index.Render(r.Context(), sb)

	// This is the worst, but templating the style attribute is
	// not allowed for security concerns.
	out := strings.Replace(
		sb.String(),
		`style="REPLACE_THIS_ETH_LOGO"`,
		mapPosition(nodes.XOffsetPercent(), nodes.YOffsetPercent()),
		1,
	)

	w.Write([]byte(out))
}

func setQuery(url *url.URL, key, value string) *url.URL {
	newURL := *url
	query := newURL.Query()
	query.Set(key, value)
	newURL.RawQuery = query.Encode()

	return &newURL
}

func (a *API) nodesListHandler(w http.ResponseWriter, r *http.Request) {
	var pageNumber int
	var networkID int64
	var synced int
	var query string
	var err error

	redirectURL := r.URL
	redirect := false

	pageNumStr := r.URL.Query().Get("page")
	networkIDStr := r.URL.Query().Get("network")
	syncedStr := r.URL.Query().Get("synced")
	query = r.URL.Query().Get("q")

	// This is a full node ID, just redirect to the node's page
	if len(query) == 64 {
		http.Redirect(w, r, "/nodes/"+query, http.StatusTemporaryRedirect)

		return
	}

	if pageNumStr == "" {
		redirectURL = setQuery(redirectURL, "page", "1")
		redirect = true
	} else {
		pageNumber, err = strconv.Atoi(pageNumStr)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad page number value: %s\n", pageNumStr)

			return
		}
		if pageNumber < 1 {
			redirectURL = setQuery(redirectURL, "page", "1")
			redirect = true
		}
	}

	if networkIDStr == "" {
		redirectURL = setQuery(redirectURL, "network", "1")
		redirect = true
	} else {
		networkID, err = strconv.ParseInt(networkIDStr, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad network id value: %s\n", networkIDStr)

			return
		}
	}

	if syncedStr == "" {
		redirectURL = setQuery(redirectURL, "synced", "1")
		redirect = true
	} else {
		synced, err = strconv.Atoi(syncedStr)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad synced value: %s\n", networkIDStr)

			return
		}

		if synced != -1 && synced != 0 && synced != 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad synced value: %s. Must be one of -1, 0, 1\n", syncedStr)

			return
		}
	}

	if redirect {
		http.Redirect(w, r, redirectURL.String(), http.StatusTemporaryRedirect)

		return
	}

	nodeListQuery, err := database.ParseNodeListQuery(query)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `Invalid query: "%s". Not a valid IP address, nor hex string.`, query)

		return
	}

	nodes, err := a.db.GetNodeList(
		r.Context(),
		pageNumber,
		networkID,
		synced,
		*nodeListQuery,
	)
	if err != nil {
		log.Error("get node list failed", "err", err, "pageNumber", pageNumber)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}

	nodeList := public.NodeList(*nodes)
	index := public.Index(nodeList, networkID, synced)
	_ = index.Render(r.Context(), w)
}

func (a *API) handleRoot(w http.ResponseWriter, r *http.Request) {
	var networkID int64
	var synced int
	var err error

	redirectURL := r.URL
	redirect := false

	networkIDStr := r.URL.Query().Get("network")
	syncedStr := r.URL.Query().Get("synced")

	if networkIDStr == "" {
		redirectURL = setQuery(redirectURL, "network", "1")
		redirect = true
	} else {
		networkID, err = strconv.ParseInt(networkIDStr, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad network id value: %s\n", networkIDStr)

			return
		}
	}

	if syncedStr == "" {
		redirectURL = setQuery(redirectURL, "synced", "1")
		redirect = true
	} else {
		synced, err = strconv.Atoi(syncedStr)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad synced value: %s\n", networkIDStr)

			return
		}

		if synced != -1 && synced != 0 && synced != 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad synced value: %s. Must be one of -1, 0, 1\n", syncedStr)

			return
		}
	}

	if redirect {
		http.Redirect(w, r, redirectURL.String(), http.StatusTemporaryRedirect)

		return
	}

	stats := a.getStats().Filter(
		func(_ int, s database.Stats) bool {
			if networkID == -1 {
				return true
			}

			if s.NetworkID == nil || s.ForkID == nil {
				return false
			}

			if *s.NetworkID != networkID {
				return false
			}

			fork, ok := database.Forks[*s.NetworkID]

			// If fork is not known, keep the stats.
			if !ok {
				return true
			}

			// If the fork is known, the fork ID should be in the set.
			_, ok = fork.Hash[*s.ForkID]
			return ok
		},
		func(_ int, s database.Stats) bool {
			return synced == -1 ||
				(synced == 1 && s.Synced == "Yes") ||
				(synced == 0 && s.Synced == "No")
		},
	)

	statsPage := public.Stats(
		networkID,
		synced,
		public.StatsGroup("Client Names", stats.CountClientName()),
		public.StatsGroup("Countries", stats.GroupCountries().Limit(20)),
		public.StatsGroup("OS / Archetectures", stats.GroupOS()),
		// public.StatsGroup("Languages", stats.GroupLanguage()),
	)

	index := public.Index(statsPage, networkID, synced)

	sb := new(strings.Builder)
	_ = index.Render(r.Context(), sb)

	out := strings.ReplaceAll(sb.String(), "STYLE_REPLACE", "style")

	_, _ = w.Write([]byte(out))
}

func (a *API) handleHistoryList(w http.ResponseWriter, r *http.Request) {
	var networkID int64
	var isError int
	var before, after *time.Time

	var err error

	redirectURL := r.URL
	redirect := false

	query := r.URL.Query()
	networkIDStr := query.Get("network")
	isErrorStr := query.Get("error")
	beforeStr := query.Get("before")
	afterStr := query.Get("after")

	if networkIDStr == "" {
		redirectURL = setQuery(redirectURL, "network", "1")
		redirect = true
	} else {
		networkID, err = strconv.ParseInt(networkIDStr, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad network id value: %s\n", networkIDStr)

			return
		}
	}

	if isErrorStr == "" {
		redirectURL = setQuery(redirectURL, "error", "-1")
		redirect = true
	} else {
		isError, err = strconv.Atoi(isErrorStr)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad error value: %s\n", networkIDStr)

			return
		}

		if isError != -1 && isError != 0 && isError != 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad error value: %s. Must be one of -1, 0, 1\n", isErrorStr)

			return
		}
	}

	if beforeStr != "" {
		beforeT, err := time.ParseInLocation(database.DateTimeLocal, beforeStr, time.UTC)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(
				w,
				"bad before value: %s. Must be YYYY-MM-DDThh:mm:ss format. It will be interpreted in UTC.\n",
				beforeStr,
			)

			return
		}

		before = &beforeT
	}

	if afterStr != "" {
		afterT, err := time.ParseInLocation(database.DateTimeLocal, afterStr, time.UTC)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(
				w,
				"bad after value: %s. Must be YYYY-MM-DDThh:mm:ss format. It will be interpreted in UTC.\n",
				afterStr,
			)

			return
		}

		after = &afterT
	}

	if beforeStr == "" && afterStr == "" {
		redirectURL = setQuery(redirectURL, "after", time.Now().UTC().Format(database.DateTimeLocal))
		redirect = true
	}

	if redirect {
		http.Redirect(w, r, redirectURL.String(), http.StatusTemporaryRedirect)

		return
	}

	historyList, err := a.db.GetHistoryList(r.Context(), before, after, networkID, isError)
	if err != nil {
		log.Error("get history list failed", "err", err)
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	index := public.Index(
		public.HistoryList(*historyList),
		networkID,
		1,
	)

	sb := new(strings.Builder)
	_ = index.Render(r.Context(), sb)

	out := strings.ReplaceAll(sb.String(), "STYLE_REPLACE", "style")

	_, _ = w.Write([]byte(out))
}

func handleFavicon(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write(public.Favicon)
}

func (a *API) handleHelp(w http.ResponseWriter, r *http.Request) {
	helpPage := public.HelpPage(a.enode)

	index := public.Index(helpPage, 1, -1)
	_ = index.Render(r.Context(), w)
}

func (a *API) StartServer(wg *sync.WaitGroup, address string) {
	defer wg.Done()

	router := http.NewServeMux()

	router.HandleFunc("/", a.handleRoot)
	router.HandleFunc("/favicon.ico", handleFavicon)
	router.HandleFunc("/history/", a.handleHistoryList)
	router.HandleFunc("/nodes/", a.nodesHandler)
	router.HandleFunc("/help/", a.handleHelp)

	log.Info("Starting API", "address", address)
	_ = http.ListenAndServe(address, router)
}
