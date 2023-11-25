package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/a-h/templ"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/public"
)

type API struct {
	db                   *database.DB
	statsUpdateFrequency time.Duration
	enode                string
	snapshotDir          string

	stats     database.AllStats
	statsLock sync.RWMutex
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

		stats:     database.AllStats{},
		statsLock: sync.RWMutex{},
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
	a.statsLock.RLock()
	defer a.statsLock.RUnlock()

	return a.stats
}

// Meant to be run as a goroutine
func (a *API) statsUpdaterDaemon() {
	for {
		start := time.Now()
		nextLoop := start.Truncate(a.statsUpdateFrequency).Add(a.statsUpdateFrequency)

		log.Debug("updating stats...")

		deleteBeforeTs := start.AddDate(0, 0, -7)
		afterTs := deleteBeforeTs

		oldStats := a.getStats()

		if len(oldStats) > 0 {
			afterTs = oldStats[len(oldStats)-1].Timestamp
		}

		stats, err := a.db.GetStats(context.Background(), afterTs, start)
		if err != nil {
			log.Error("stats updater daemon: get stats failed", "err", err)
			time.Sleep(time.Minute)

			continue
		}

		oldStats = append(oldStats, stats...)

		for i, stat := range oldStats {
			if stat.Timestamp.After(deleteBeforeTs) {
				a.replaceStats(oldStats[i:])

				break
			}
		}

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
	index := public.Index(public.URLFromReq(r), nodeTable, 1, -1)
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

func parseAllYesNoParam(w http.ResponseWriter, str string, param string) (int, bool) {
	switch str {
	case "":
		return 1, true
	case "all":
		return -1, true
	case "yes":
		return 1, true
	case "no":
		return 0, true
	}

	synced, err := strconv.Atoi(str)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad %s value: %s\n", param, str)

		return 0, false
	}

	if synced != -1 && synced != 0 && synced != 1 {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad %s value: %s. Must be one of all, yes, no\n", param, str)

		return 0, false
	}

	return synced, true
}

func parseSyncedParam(w http.ResponseWriter, str string) (int, bool) {
	return parseAllYesNoParam(w, str, "synced")
}

func parseErrorParam(w http.ResponseWriter, str string) (int, bool) {
	return parseAllYesNoParam(w, str, "error")
}

func parsePageNum(w http.ResponseWriter, pageNumStr string) (int, bool) {
	if pageNumStr == "" {
		return 1, true
	}

	pageNumber, err := strconv.Atoi(pageNumStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad page number value: %s\n", pageNumStr)

		return 0, false
	}

	if pageNumber <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad page number value, must be greater than 0: %d", pageNumber)

		return 0, false
	}

	return pageNumber, true
}

func parseNetworkID(w http.ResponseWriter, networkIDStr string) (int64, bool) {
	if networkIDStr == "" {
		return 1, true
	}

	networkID, err := strconv.ParseInt(networkIDStr, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, "bad network id value: %s\n", networkIDStr)

		return 0, false
	}

	return networkID, true
}

func (a *API) nodesListHandler(w http.ResponseWriter, r *http.Request) {
	urlQuery := r.URL.Query()

	pageNumStr := urlQuery.Get("page")
	networkIDStr := urlQuery.Get("network")
	syncedStr := urlQuery.Get("synced")
	query := urlQuery.Get("q")
	clientName := urlQuery.Get("client_name")
	clientUserData := urlQuery.Get("client_user_data")

	// This is a full node ID, just redirect to the node's page
	if len(query) == 64 {
		http.Redirect(w, r, "/nodes/"+query, http.StatusTemporaryRedirect)

		return
	}

	pageNumber, ok := parsePageNum(w, pageNumStr)
	if !ok {
		return
	}

	networkID, ok := parseNetworkID(w, networkIDStr)
	if !ok {
		return
	}

	synced, ok := parseSyncedParam(w, syncedStr)
	if !ok {
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
		clientName,
		clientUserData,
	)
	if err != nil {
		log.Error("get node list failed", "err", err, "pageNumber", pageNumber)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintln(w, "Internal Server Error")

		return
	}

	reqURL := public.URLFromReq(r)

	nodeList := public.NodeList(reqURL, *nodes)
	index := public.Index(reqURL, nodeList, networkID, synced)
	_ = index.Render(r.Context(), w)
}

func (a *API) handleRoot(w http.ResponseWriter, r *http.Request) {
	networkIDStr := r.URL.Query().Get("network")
	syncedStr := r.URL.Query().Get("synced")

	networkID, ok := parseNetworkID(w, networkIDStr)
	if !ok {
		return
	}

	synced, ok := parseSyncedParam(w, syncedStr)
	if !ok {
		return
	}

	allStats := a.getStats().Filter(
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
				(synced == 1 && s.Synced) ||
				(synced == 0 && !s.Synced)
		},
	)

	reqURL := public.URLFromReq(r)

	clientNames := allStats.CountClientName()
	countries := allStats.GroupCountries()
	OSs := allStats.GroupOS()

	statsPage := public.Stats(
		reqURL,
		networkID,
		synced,
		[]templ.Component{
			public.StatsGraph("Client Names", clientNames.ClientNameTimeseries()),
		},
		[]templ.Component{
			public.StatsGroup("Client Names", clientNames.Last()),
			public.StatsGroup("Countries", countries.Last().Limit(20)),
			public.StatsGroup("OS / Archetectures", OSs.Last()),
		},
	)

	index := public.Index(reqURL, statsPage, networkID, synced)

	sb := new(strings.Builder)
	_ = index.Render(r.Context(), sb)

	out := strings.ReplaceAll(sb.String(), "STYLE_REPLACE", "style")

	_, _ = w.Write([]byte(out))
}

func (a *API) handleHistoryList(w http.ResponseWriter, r *http.Request) {
	var before, after *time.Time

	query := r.URL.Query()
	networkIDStr := query.Get("network")
	isErrorStr := query.Get("error")
	beforeStr := query.Get("before")
	afterStr := query.Get("after")

	networkID, ok := parseNetworkID(w, networkIDStr)
	if !ok {
		return
	}

	isError, ok := parseErrorParam(w, isErrorStr)
	if !ok {
		return
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
		afterT := time.Now().UTC()
		after = &afterT
	}

	historyList, err := a.db.GetHistoryList(r.Context(), before, after, networkID, isError)
	if err != nil {
		log.Error("get history list failed", "err", err)
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	reqURL := public.URLFromReq(r)

	index := public.Index(
		reqURL,
		public.HistoryList(reqURL, *historyList),
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

	index := public.Index(public.URLFromReq(r), helpPage, 1, -1)
	_ = index.Render(r.Context(), w)
}

func allFiles(dirName string) ([]fs.FileInfo, error) {
	dir, err := os.ReadDir(dirName)
	if err != nil {
		return nil, fmt.Errorf("reading dir: %s failed: %w", dirName, err)
	}

	files := []fs.FileInfo{}
	for _, file := range dir {
		if file.IsDir() {
			continue
		}

		// Not a complete snapshot
		if !strings.HasSuffix(file.Name(), ".gz") {
			continue
		}

		info, err := file.Info()
		if err != nil {
			return nil, fmt.Errorf("file info for: %s failed: %w", file.Name(), err)
		}

		files = append(files, info)
	}

	slices.SortStableFunc(files, func(a, b fs.FileInfo) int {
		return strings.Compare(b.Name(), a.Name())
	})

	return files, nil
}

func (a *API) handleSnapshotsList(w http.ResponseWriter, r *http.Request) {
	files, err := allFiles(a.snapshotDir)
	if err != nil {
		log.Error("listing snapshots failed", "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Internal Server Error\n")

		return
	}

	snapshotList := public.SnapshotsList(files)

	index := public.Index(public.URLFromReq(r), snapshotList, 1, 1)
	_ = index.Render(r.Context(), w)
}

func (a *API) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	urlPath := r.URL.Path

	split := strings.Split(strings.Trim(urlPath, "/"), "/")

	if len(split) == 1 {
		a.handleSnapshotsList(w, r)

		return
	}

	if len(split) == 2 {
		fileServer := http.FileServer(http.Dir(a.snapshotDir))

		rCopy := r.Clone(r.Context())
		rCopy.URL.Path = split[1]
		fileServer.ServeHTTP(w, rCopy)

		return
	}

	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprint(w, "Bad Request")
}

func (a *API) StartServer(wg *sync.WaitGroup, address string) {
	defer wg.Done()

	router := http.NewServeMux()

	router.HandleFunc("/", a.handleRoot)
	router.HandleFunc("/favicon.ico", handleFavicon)
	router.HandleFunc("/snapshots/", a.handleSnapshots)
	router.HandleFunc("/help/", a.handleHelp)
	router.HandleFunc("/history/", a.handleHistoryList)
	router.HandleFunc("/nodes/", a.nodesHandler)

	log.Info("Starting API", "address", address)
	_ = http.ListenAndServe(address, router)
}
