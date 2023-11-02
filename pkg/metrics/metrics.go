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
			"error",
		},
	)
	DatabaseStats = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "database_stats",
			Help:      "Stats from the database",
		},
		[]string{
			"stat_name",
		},
	)
	DatabaseRetries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "database_retries_total",
			Help:      "Number of retries per retry count",
		},
		[]string{
			"retry",
		},
	)
)

func boolToStatus(b bool) string {
	if b {
		return "success"
	}

	return "error"
}

func ObserveDBQuery(queryName string, start time.Time, err error) {
	dbQueryHistogram.With(prometheus.Labels{
		"query_name": queryName,
		"status":     boolToStatus(err == nil),
	}).Observe(time.Since(start).Seconds())
}

func NodeUpdateInc(direction string, err string) {
	nodeUpdateCount.With(prometheus.Labels{
		"direction": direction,
		"status":    boolToStatus(err == ""),
		"error":     err,
	}).Inc()
}
