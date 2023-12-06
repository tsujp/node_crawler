package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/a-h/templ"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/public"
)

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

	params := fmt.Sprintf("%d,%d", networkID, synced)

	b, ok := a.getCache(params)
	if ok {
		_, _ = w.Write(b)

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
	dialSuccess := allStats.GroupDialSuccess()

	statsPage := public.Stats(
		reqURL,
		networkID,
		synced,
		[]templ.Component{
			public.StatsGraph("Client Names (7d)", "client_names", clientNames.Timeseries().Percentage()),
			public.StatsGraph("Dial Success (7d)", "dial_success", dialSuccess.Timeseries().Percentage().Colours("#05c091", "#ff6e76")),
		},
		[]templ.Component{
			public.StatsGroup("Client Names", clientNames.Last()),
			public.StatsGroup("Countries", countries.Last()),
			public.StatsGroup("OS / Archetectures", OSs.Last()),
		},
		len(clientNames) == 0,
	)

	index := public.Index(reqURL, statsPage, networkID, synced)

	sb := new(strings.Builder)
	_ = index.Render(r.Context(), sb)

	out := strings.ReplaceAll(sb.String(), "STYLE_REPLACE", "style")

	_, _ = w.Write([]byte(out))

	a.setCache(params, []byte(out))
}
