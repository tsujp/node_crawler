package api

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

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
