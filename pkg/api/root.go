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
	nextForkStr := r.URL.Query().Get("next-fork")

	networkID, found := parseNetworkID(w, networkIDStr)
	if !found {
		return
	}

	synced, found := parseSyncedParam(w, syncedStr)
	if !found {
		return
	}

	nextFork, found := parseNextForkParam(w, nextForkStr)
	if !found {
		return
	}

	params := fmt.Sprintf("%d,%d,%d", networkID, synced, nextFork)

	b, found := a.getCache(params)
	if found {
		_, _ = w.Write(b)

		return
	}

	fork, forkFound := database.Forks[networkID]

	allStats := a.getStats().Filter(
		func(_ int, s database.Stats) bool {
			return synced == -1 ||
				(synced == 1 && s.Synced) ||
				(synced == 0 && !s.Synced)
		},
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

			// If fork is not known, keep the stats.
			if !forkFound {
				return true
			}

			// If the fork is known, the fork ID should be in the set.
			_, found = fork.Hash[*s.ForkID]
			return found
		},
		func(_ int, s database.Stats) bool {
			if nextFork == -1 {
				return true
			}

			if s.NextForkID == nil {
				return false
			}

			// Unknown chain, keep the stats.
			if !forkFound {
				return true
			}

			// Fork time unknown, keep the stats.
			if fork.NextFork == nil {
				return true
			}

			isReady := *s.NextForkID == *fork.NextFork

			return isReady == (nextFork == 1)
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
		nextFork,
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
