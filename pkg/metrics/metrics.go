package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	namespace = "node_crawler"

	dbQueryHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "db_query_seconds",
			Help:      "Seconds spent on each database query",
		},
		[]string{
			"query_name",
			"status",
		},
	)
	DiscUpdateBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "disc_update_backlog",
		Help:      "Number of discovered nodes in the backlog",
	})
	DiscUpdateCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "disc_update_total",
			Help:      "Number of discovered nodes",
		},
		[]string{
			"disc_version",
		},
	)
	NodeUpdateBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "node_update_backlog",
		Help:      "Number of nodes to be updated in the backlog",
	})
	nodeUpdateCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "node_update_total",
			Help:      "Number of updated nodes",
		},
		[]string{
			"direction",
			"status",
		},
	)
	TableStatsDiscNodes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "table_stats",
		Name:      "disc_nodes",
		Help:      "Number of discovered nodes",
	})
	TableStatsNodesToCrawl = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "table_stats",
		Name:      "nodes_to_crawl",
		Help:      "Number of nodes where the next crawl time is in the past",
	})
	TableStatsCrawledNodeCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "table_stats",
		Name:      "crawled_nodes",
		Help:      "Number of crawled nodes",
	})
)

func boolToStatus(b bool) string {
	if b {
		return "success"
	}

	return "error"
}

func ObserveDBQuery(queryName string, duration time.Duration, err error) {
	dbQueryHistogram.With(prometheus.Labels{
		"query_name": queryName,
		"status":     boolToStatus(err == nil),
	}).Observe(duration.Seconds())
}

func NodeUpdateInc(direction string, isErr bool) {
	nodeUpdateCount.With(prometheus.Labels{
		"direction": direction,
		"status":    boolToStatus(!isErr),
	}).Inc()
}
