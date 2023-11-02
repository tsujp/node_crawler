package api

import (
	"database/sql"
	"encoding/json"
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
	"github.com/gorilla/mux"
	lru "github.com/hashicorp/golang-lru"
)

type Api struct {
	address string
	cache   *lru.Cache
	db      *sql.DB
	dbv2    *database.DB
}

func New(address string, dbv2 *database.DB) *Api {
	cache, err := lru.New(256)
	if err != nil {
		return nil
	}

	api := &Api{
		address: address,
		cache:   cache,
		db:      nil,
		dbv2:    dbv2,
	}

	return api
}

func (a *Api) dropCacheLoop() {
	// Drop the cache every 2 minutes
	ticker := time.NewTicker(2 * time.Minute)
	for range ticker.C {
		log.Info("Dropping Cache")
		c, err := lru.New(256)
		if err != nil {
			panic(err)
		}
		a.cache = c
	}
}

func mapPosition(x, y int) string {
	return fmt.Sprintf(
		`style="position: absolute; top: %d%%; left: %d%%; transform: translate(-50%%, -50%%)"`,
		y,
		x,
	)
}

func (a *Api) nodesHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["id"]

	nodes, err := a.dbv2.GetNodeTable(r.Context(), nodeID)
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
	index := public.Index(nodeTable)
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

func (a *Api) nodesListHandler(w http.ResponseWriter, r *http.Request) {
	var pageNumber int
	var networkID int64
	var synced int
	var err error

	redirectURL := r.URL
	redirect := false

	pageNumStr := r.URL.Query().Get("page")
	networkIDStr := r.URL.Query().Get("network")
	syncedStr := r.URL.Query().Get("synced")

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
		redirectURL = setQuery(redirectURL, "synced", "-1")
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
		w.Header().Add("Location", redirectURL.String())
		w.WriteHeader(http.StatusTemporaryRedirect)

		return
	}

	nodes, err := a.dbv2.GetNodeList(r.Context(), pageNumber, networkID, synced)
	if err != nil {
		log.Error("get node list failed", "err", err, "pageNumber", pageNumber)
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	nodeList := public.NodeList(*nodes)
	index := public.Index(nodeList)
	_ = index.Render(r.Context(), w)
}

func (a *Api) handleRoot(w http.ResponseWriter, r *http.Request) {
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
		redirectURL = setQuery(redirectURL, "synced", "-1")
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
		w.Header().Add("Location", redirectURL.String())
		w.WriteHeader(http.StatusTemporaryRedirect)

		return
	}

	stats, err := a.dbv2.GetStats(r.Context())
	if err != nil {
		log.Error("get stats failed", "err", err)
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	clientNames := stats.CountClientName(
		func(s database.Stats) bool {
			return networkID == -1 || (s.NetworkID != nil && *s.NetworkID == networkID)
		},
		func(s database.Stats) bool {
			return synced == -1 ||
				(synced == 1 && s.Synced == "Yes") ||
				(synced == 0 && s.Synced == "No")
		},
	)

	maxValue := 0
	for _, client := range clientNames {
		maxValue = max(maxValue, client.Count)
	}

	sb := new(strings.Builder)

	statsPage := public.Stats(clientNames, maxValue, networkID, synced)
	index := public.Index(statsPage)
	_ = index.Render(r.Context(), sb)

	out := strings.ReplaceAll(sb.String(), "STYLE_REPLACE", "style")

	_, _ = w.Write([]byte(out))
}

func (a *Api) HandleRequests(wg *sync.WaitGroup) {
	defer wg.Done()
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", a.handleRoot)
	router.HandleFunc("/v1/dashboard", a.handleDashboard).Queries("filter", "{filter}")
	router.HandleFunc("/v1/dashboard", a.handleDashboard)
	router.HandleFunc("/nodes", a.nodesListHandler)
	router.HandleFunc("/nodes/{id}", a.nodesHandler)

	log.Info("Starting API", "address", a.address)
	_ = http.ListenAndServe(a.address, router)
}

type client struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func addFilterArgs(vars map[string]string) (string, []interface{}, error) {
	filter := strings.TrimSpace(vars["filter"])
	if filter == "" {
		return "", nil, nil
	}

	query := "FALSE "
	var filterArgs [][]string
	err := json.Unmarshal([]byte(filter), &filterArgs)
	if err != nil {
		return "", nil, err
	}

	var args []interface{}
	for _, outer := range filterArgs {
		inner := ""
		for idx, arg := range outer {
			key, value, comp, err := unmarshalFilterArgs(arg)
			if err != nil {
				return "", nil, err
			}
			if validateKey(key) {
				inner += fmt.Sprintf("(%v %v ?) ", key, comp)
				args = append(args, value)
			}
			if idx < len(outer)-1 {
				inner += " AND "
			}
		}
		if len(inner) > 0 {
			query += fmt.Sprintf("OR (%v) ", inner)
		}
	}
	return query, args, nil
}

// filter args are marshalled as key:value or key:value:comp
// with comp being a comparator (eq, gt, lt, gte, lte, not)
func unmarshalFilterArgs(arg string) (string, string, string, error) {
	split := strings.Split(arg, ":")
	if len(split) == 3 {
		comp := "="
		switch split[2] {
		case "eq":
			comp = "="
		case "lt":
			comp = "<"
		case "lte":
			comp = "<="
		case "gt":
			comp = ">"
		case "gte":
			comp = ">="
		case "not":
			comp = "!="
		default:
		}
		return split[0], split[1], comp, nil
	} else if len(split) == 2 {
		return split[0], split[1], "=", nil
	}
	return "", "", "", fmt.Errorf("umarshalling failed, got %v want 2 or 3 args", len(split))
}

type result struct {
	Clients          []client `json:"clients"`
	Languages        []client `json:"languages"`
	OperatingSystems []client `json:"operatingSystems"`
	Versions         []client `json:"versions"`
	Countries        []client `json:"countries"`
}

func (a *Api) cachedOrQuery(prefix, query string, whereArgs []interface{}) []client {
	var result []client
	if cl, ok := a.cache.Get(prefix + toQuery(query, whereArgs)); ok {
		result = cl.([]client)
	} else {
		var err error
		result, err = clientQuery(a.db, query, whereArgs...)
		if err != nil {
			log.Error("Failure in the query", "err", err)
		}
	}
	return result
}

func toQuery(query string, whereArgs []interface{}) string {
	var res string
	queryParts := strings.Split(query, "?")
	for idx, s := range queryParts {
		res += s
		if idx == len(whereArgs) {
			break
		}
		res += whereArgs[idx].(string)
	}
	return res
}

func (a *Api) storeCache(
	clientQuery,
	languageQuery,
	osQuery,
	countryQuery,
	versionQuery string,
	whereArgs []interface{},
	r result,
) {
	a.cache.Add("c"+toQuery(clientQuery, whereArgs), r.Clients)
	a.cache.Add("l"+toQuery(languageQuery, whereArgs), r.Languages)
	a.cache.Add("o"+toQuery(osQuery, whereArgs), r.OperatingSystems)
	a.cache.Add("v"+toQuery(versionQuery, whereArgs), r.Versions)
	a.cache.Add("co"+toQuery(versionQuery, whereArgs), r.Countries)
}

func (a *Api) handleDashboard(rw http.ResponseWriter, r *http.Request) {
	// Set's the cache to 10 minutes, which matches the same as the crawler.
	rw.Header().Set("Cache-Control", "max-age=600")

	vars := mux.Vars(r)

	nameCountInQuery := strings.Count(vars["filter"], "\"name:")

	// Where
	where, whereArgs, err := addFilterArgs(vars)
	if err != nil {
		log.Error("Failure when adding filter to the query", "err", err)
		return
	}
	if whereArgs != nil {
		where = "WHERE " + where
	}

	var topLanguageQuery string
	if nameCountInQuery == 1 {
		topLanguageQuery = fmt.Sprintf(`
			SELECT
				Name,
				Count(*) as Count
			FROM (
				SELECT
					language_name || language_version as Name
				FROM nodes %v
			)
			GROUP BY Name
			ORDER BY Count DESC
		`, where)
	} else {
		topLanguageQuery = fmt.Sprintf(`
			SELECT
				language_name as Name,
				COUNT(language_name) as Count
			FROM nodes %v
			GROUP BY language_name
			ORDER BY Count DESC
		`, where)
	}

	topClientsQuery := fmt.Sprintf(`
		SELECT
			name as Name,
			COUNT(name) as Count
		FROM nodes %v
		GROUP BY name
		ORDER BY count DESC
	`, where)
	topOsQuery := fmt.Sprintf(`
		SELECT
			os_name as Name,
			COUNT(os_name) as Count
		FROM nodes %v
		GROUP BY os_name
		ORDER BY count DESC
	`, where)
	topVersionQuery := fmt.Sprintf(`
		SELECT
			Name,
			Count(*) as Count
		FROM (
			SELECT
				version_major || '.' || version_minor || '.' || version_patch as Name
			FROM nodes %v
		)
		GROUP BY Name
		ORDER BY Count DESC
	`, where)
	topCountriesQuery := fmt.Sprintf(`
		SELECT
			country_name as Name,
			COUNT(country_name) as Count
		FROM nodes %v
		GROUP BY country_name
		ORDER BY count DESC
	`, where)

	clients := a.cachedOrQuery("c", topClientsQuery, whereArgs)
	language := a.cachedOrQuery("l", topLanguageQuery, whereArgs)
	operatingSystems := a.cachedOrQuery("o", topOsQuery, whereArgs)
	countries := a.cachedOrQuery("co", topCountriesQuery, whereArgs)

	var versions []client
	if nameCountInQuery == 1 {
		versions = a.cachedOrQuery("v", topVersionQuery, whereArgs)
	}

	res := result{
		Clients:          clients,
		Languages:        language,
		OperatingSystems: operatingSystems,
		Versions:         versions,
		Countries:        countries,
	}
	a.storeCache(
		topClientsQuery,
		topLanguageQuery,
		topOsQuery,
		topCountriesQuery,
		topVersionQuery,
		whereArgs,
		res,
	)
	json.NewEncoder(rw).Encode(res)
}

func clientQuery(db *sql.DB, query string, args ...interface{}) ([]client, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	var clients []client
	for rows.Next() {
		var cl client
		err = rows.Scan(&cl.Name, &cl.Count)
		if err != nil {
			return nil, err
		}
		clients = append(clients, cl)
	}
	return clients, nil
}

func validateKey(key string) bool {
	validKeys := map[string]struct{}{
		"id":               {},
		"name":             {},
		"version_major":    {},
		"version_minor":    {},
		"version_patch":    {},
		"version_tag":      {},
		"version_build":    {},
		"version_date":     {},
		"os_name":          {},
		"os_architecture":  {},
		"language_name":    {},
		"language_version": {},
		"country":          {},
	}
	_, ok := validKeys[key]
	return ok
}
