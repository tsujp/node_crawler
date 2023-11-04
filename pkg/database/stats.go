package database

import (
	"strings"

	"golang.org/x/exp/slices"
)

type Stats struct {
	Client    Client
	NetworkID *int64
	Country   *string
	Synced    string
}

func (s Stats) CountryStr() string {
	if s.Country == nil {
		return Unknown
	}

	return *s.Country
}

type AllStats []Stats

type KeyFn func(Stats) string
type StatsFilterFn func(int, Stats) bool

func (s AllStats) Filter(filters ...StatsFilterFn) AllStats {
	out := make(AllStats, 0, len(s))

	for i, stat := range s {
		skip := false
		for _, filter := range filters {
			if !filter(i, stat) {
				skip = true
				break
			}
		}

		if !skip {
			out = append(out, stat)
		}
	}

	return out
}

func (s AllStats) GroupBy(keyFn KeyFn, filters ...StatsFilterFn) CountTotal {
	count := map[string]int{}

	for _, stat := range s.Filter(filters...) {
		key := keyFn(stat)

		v, ok := count[key]
		if !ok {
			v = 0
		}

		v += 1
		count[key] = v
	}

	out := make([]Count, 0, len(count))
	total := 0

	for key, value := range count {
		out = append(out, Count{
			Key:   key,
			Count: value,
		})

		total += value
	}

	slices.SortFunc(out, func(a, b Count) int {
		if a.Count == b.Count {
			return strings.Compare(b.Key, a.Key)
		}

		return b.Count - a.Count
	})

	return CountTotal{
		Values: out,
		Total:  total,
	}
}

type Count struct {
	Key   string
	Count int
}

type CountTotal struct {
	Values []Count
	Total  int
}

func (t CountTotal) Limit(limit int) CountTotal {
	return CountTotal{
		Values: t.Values[:min(limit, len(t.Values))],
		Total:  t.Total,
	}
}

type CountTotalOrderFn func(a, b Count) int

func (t CountTotal) OrderBy(orderByFn CountTotalOrderFn) CountTotal {
	slices.SortStableFunc(t.Values, orderByFn)

	return CountTotal{
		Values: t.Values,
		Total:  t.Total,
	}
}

func (s AllStats) CountClientName(filters ...StatsFilterFn) CountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Name
		},
		filters...,
	)
}

func (s AllStats) GroupCountries(filters ...StatsFilterFn) CountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.CountryStr()
		},
		filters...,
	)
}

func (s AllStats) GroupOS(filters ...StatsFilterFn) CountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.OS + " / " + s.Client.Arch
		},
		filters...,
	)
}

func (s AllStats) GroupArch(filters ...StatsFilterFn) CountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Arch
		},
		filters...,
	)
}

func (s AllStats) GroupLanguage(filters ...StatsFilterFn) CountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Language
		},
		filters...,
	)
}
