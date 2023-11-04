package database

import (
	"encoding/hex"
	"math"
	"time"
)

func int64PrtToTimePtr(i *int64) *time.Time {
	if i == nil {
		return nil
	}

	u := time.Unix(*i, 0)
	return &u
}

type NodeListRow struct {
	nodeID            []byte
	UpdatedAt         *time.Time
	ClientName        *string
	Country           *string
	HeadHashTimestamp *time.Time
}

func (n NodeListRow) NodeID() string {
	return hex.EncodeToString(n.nodeID)
}

func (n NodeListRow) SinceUpdate() string {
	return sinceUpdate(n.UpdatedAt)
}

func (n NodeListRow) IsSynced() string {
	return isSynced(n.UpdatedAt, n.HeadHashTimestamp)
}

type NodeList struct {
	PageNumber    int
	PageSize      int
	Synced        int
	Offset        int
	Total         int
	NetworkFilter int64
	Query         string
	List          []NodeListRow

	Networks []int64
}

func (l NodeList) NPages() int {
	return int(math.Ceil(float64(l.Total) / float64(l.PageSize)))
}
