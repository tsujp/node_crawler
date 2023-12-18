package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/public"
	"github.com/ethereum/node-crawler/public/components"
)

func (a *API) handleRoot(w http.ResponseWriter, r *http.Request) {
	index := components.Index22()

	sb := new(strings.Builder)
	_ = index.Render(r.Context(), sb)

	out := strings.ReplaceAll(sb.String(), "STYLE_REPLACE", "style")

	log.Info(out)

	_, _ = w.Write([]byte(out))
}

func (a *API) handleRoot22(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	networkIDStr := query.Get("network")
	syncedStr := query.Get("synced")
	nextForkStr := query.Get("next-fork")
	clientName := query.Get("client-name")

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

	params := fmt.Sprintf("%s,%d,%d,%d", clientName, networkID, synced, nextFork)

	b, found := a.getCache(params)
	if found {
		_, _ = w.Write(b)

		return
	}

	fork, forkFound := database.Forks[networkID]

	days := 3

	// All network ids has so much data, so we're prioritizing speed over days
	// of data.
	if networkID == -1 || synced == -1 {
		days = 1
	}

	before := time.Now().Truncate(30 * time.Minute).Add(30 * time.Minute)
	after := before.AddDate(0, 0, -days)

	n := time.Now()

	allStats, err := a.db.GetStats(r.Context(), after, before, networkID, synced)
	if err != nil {
		log.Error("GetStats failed", "err", err)

		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, "Internal Server Error")

		return
	}

	log.Info("db done", "duration", time.Since(n))

	allStats = allStats.Filter(
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
		func(_ int, s database.Stats) bool {
			if clientName == "" {
				return true
			}

			return s.Client.Name == clientName
		},
	)

	log.Info("filter", "duration", time.Since(n))

	reqURL := public.URLFromReq(r)

	graphs := make([]templ.Component, 0, 2)
	last := make([]templ.Component, 0, 4)

	if clientName == "" {
		clientNames := allStats.GroupClientName()

		graphs = append(
			graphs,
			public.StatsGraph(
				fmt.Sprintf("Client Names (%dd)", days),
				"client_names",
				clientNames.Timeseries().Percentage(),
			),
		)

		last = append(
			last,
			public.StatsGroup(
				"Client Names",
				clientNames.Last(),
				func(key string) templ.SafeURL {
					return reqURL.
						KeepParams("network", "synced", "next-fork").
						WithParam("client-name", key).
						SafeURL()
				},
			),
		)
	} else {
		clientVersions := allStats.GroupClientVersion()

		graphs = append(
			graphs,
			public.StatsGraph(
				fmt.Sprintf("Client Versions (%dd)", days),
				"client_versions",
				clientVersions.Timeseries().Percentage(),
			),
		)

		last = append(
			last,
			public.StatsGroup(
				"Client Versions",
				clientVersions.Last(),
				func(_ string) templ.SafeURL { return "" },
			),
		)
	}

	countries := allStats.GroupCountries()
	OSs := allStats.GroupOS()

	last = append(
		last,
		public.StatsGroup(
			"Countries",
			countries.Last(),
			func(_ string) templ.SafeURL { return "" },
		),
		public.StatsGroup(
			"OS / Architectures",
			OSs.Last(),
			func(_ string) templ.SafeURL { return "" },
		),
	)

	dialSuccess := allStats.GroupDialSuccess()

	graphs = append(
		graphs,
		public.StatsGraph(
			fmt.Sprintf("Dial Success (%dd)", days),
			"dial_success",
			dialSuccess.Timeseries().Percentage().Colours("#05c091", "#ff6e76"),
		),
	)

	log.Info("group", "duration", time.Since(n))

	// TODO: Temporary comment-out.
	//statsPage := public.Stats(
	//	reqURL,
	//	networkID,
	//	synced,
	//	nextFork,
	//	clientName,
	//	graphs,
	//	last,
	//	len(allStats) == 0,
	//)

	log.Info("page", "duration", time.Since(n))

	// TODO: Replace properly.
	// index := public.Index(reqURL, statsPage, networkID, synced)

	// Temporary.
	index := components.Index22()

	sb := new(strings.Builder)
	_ = index.Render(r.Context(), sb)

	out := strings.ReplaceAll(sb.String(), "STYLE_REPLACE", "style")

	_, _ = w.Write([]byte(out))

	// Cache the result until 1 minute after the end timestamp.
	// The new stats should have been generated by then.
	a.setCache(params, []byte(out), before.Add(time.Minute))
}
