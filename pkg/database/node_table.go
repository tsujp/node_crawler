package database

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/params"
)

type NodeTableHistory struct {
	CrawledAt time.Time
	Direction string
	Error     string
}

type NodeTable struct {
	nodeID         []byte
	updatedAt      *time.Time
	Enode          *string
	ClientName     *string
	RlpxVersion    *int64
	Capabilities   *string
	networkID      *int64
	ForkID         *ForkID
	NextForkID     *uint64
	HeadHash       *[]byte
	HeadHashTime   *time.Time
	IP             *string
	ConnectionType *string
	Country        *string
	City           *string
	Latitude       *float64
	Longitude      *float64
	nextCrawl      *time.Time

	History []NodeTableHistory
}

func (n NodeTable) RLPXVersion() string {
	if n.RlpxVersion == nil {
		return ""
	}

	return strconv.FormatInt(*n.RlpxVersion, 10)
}

func (n NodeTable) ForkIDStr() string {
	if n.ForkID == nil {
		return ""
	}

	name := Unknown

	if n.networkID != nil {
		forkData, ok := Forks[*n.networkID]
		if ok {
			forkName, ok := forkData.Hash[n.ForkID.Uint32()]
			if ok {
				name = forkName
			}
		}
	}

	return fmt.Sprintf("%s (%s)", name, n.ForkID.Hex())
}

func (n NodeTable) NextForkIDStr() string {
	if n.NextForkID == nil {
		return ""
	}

	name := Unknown

	if n.networkID != nil {
		forkData, ok := Forks[*n.networkID]
		if ok {
			forkName, ok := forkData.BlockTime[*n.NextForkID]
			if ok {
				name = forkName
			}
		}
	}

	return fmt.Sprintf("%s (%d)", name, *n.NextForkID)
}

func isReadyForCancun(networkID *int64, forkID *uint32, nextForkID *uint64) int {
	if networkID == nil || nextForkID == nil {
		return -1
	}

	chain, ok := Chains[*networkID]
	if !ok {
		return -1
	}

	if nextForkID == chain.CancunTime {
		return 1
	}

	if forkID == nil {
		return -1
	}

	forks, ok := Forks[*networkID]
	if !ok {
		return -1
	}

	if forks.Hash[*forkID] == "Cancun" {
		return 1
	}

	return 0
}

func StringOrEmpty(v *string) string {
	if v == nil {
		return ""
	}

	return *v
}

func IntOrEmpty[T int | int64](v *T) string {
	if v == nil {
		return ""
	}

	return strconv.FormatInt(int64(*v), 10)
}

func (n NodeTable) NodeID() string {
	return hex.EncodeToString(n.nodeID)
}

func (n NodeTable) NetworkID() string {
	if n.networkID == nil {
		return ""
	}

	return fmt.Sprintf("%s (%d)", NetworkName(n.networkID), *n.networkID)
}

func (n NodeTable) YOffsetPercent() int {
	if n.Latitude == nil {
		return 0
	}

	return 100 - int((*n.Latitude+90)/180*100)
}

func (n NodeTable) XOffsetPercent() int {
	if n.Longitude == nil {
		return 0
	}

	return int((*n.Longitude + 180) / 360 * 100)
}

var DateFormat = "2006-01-02 15:04:05 MST"

func (n NodeTable) HeadHashLine() string {
	if n.HeadHashTime == nil {
		return hex.EncodeToString(*n.HeadHash)
	}

	return fmt.Sprintf(
		"%s (%s)",
		hex.EncodeToString(*n.HeadHash),
		n.HeadHashTime.UTC().Format(DateFormat),
	)
}

var Unknown = "Unknown"

func isSynced(updatedAt *time.Time, headHash *time.Time) string {
	if updatedAt == nil || headHash == nil {
		return Unknown
	}

	// If head hash is within one minute of the crawl time,
	// we can consider the node in sync
	if updatedAt.Sub(*headHash).Abs() < time.Minute {
		return "Yes"
	}

	return "No"
}

func (n NodeTable) IsSynced() string {
	return isSynced(n.updatedAt, n.HeadHashTime)
}

func sinceUpdate(updatedAt *time.Time) string {
	if updatedAt == nil {
		return "Never"
	}

	since := time.Since(*updatedAt)
	if since < 0 {
		return "In " + (-since).Truncate(time.Second).String()
	}

	return since.Truncate(time.Second).String() + " ago"
}

func (n NodeTable) UpdatedAt() string {
	return fmt.Sprintf(
		"%s (%s)",
		sinceUpdate(n.updatedAt),
		n.updatedAt.UTC().Format(DateFormat),
	)
}

func (n NodeTable) NextCrawl() string {
	if n.nextCrawl == nil {
		return "Never"
	}

	return fmt.Sprintf("%s (%s)", sinceUpdate(n.nextCrawl), n.nextCrawl.UTC().Format(DateFormat))
}

func NetworkName(networkID *int64) string {
	if networkID == nil {
		return Unknown
	}

	switch *networkID {
	case -1:
		return "All"
	case params.MainnetChainConfig.ChainID.Int64():
		return "Mainnet"
	case params.HoleskyChainConfig.ChainID.Int64():
		return "Holešky"
	case params.SepoliaChainConfig.ChainID.Int64():
		return "Sepolia"
	case params.GoerliChainConfig.ChainID.Int64():
		return "Goerli"
	default:
		return Unknown
	}
}

func (n NodeTable) NetworkName() string {
	return NetworkName(n.networkID)
}
