package crawlerv2

import (
	"crypto/ecdsa"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/crawler"
	"github.com/ethereum/node-crawler/pkg/database"
)

func nodeIDString(start string, c byte) string {
	out := make([]byte, 64)

	for i, r := range []byte(start) {
		out[i] = r
	}

	for i := len(start); i < 64; i++ {
		out[i] = c
	}

	return string(out)
}

type CrawlerV2 struct {
	db        *database.DB
	nodeKey   *ecdsa.PrivateKey
	genesis   *core.Genesis
	networkID uint64

	wg *sync.WaitGroup
}

func NewCrawlerV2(
	db *database.DB,
	nodeKey *ecdsa.PrivateKey,
	genesis *core.Genesis,
	networkID uint64,
) (*CrawlerV2, error) {
	crawler := &CrawlerV2{
		db:        db,
		nodeKey:   nodeKey,
		genesis:   genesis,
		networkID: networkID,
	}

	crawler.wg = new(sync.WaitGroup)

	return crawler, nil
}

func (c *CrawlerV2) Wait() {
	c.wg.Wait()
}

var hexAlpha = "0123456789abcdef"

type Range struct {
	start string
	end   string
}

func Range16() []Range {
	out := make([]Range, 16)

	for i := 0; i < 16; i++ {
		prefix := string([]byte{hexAlpha[i]})

		out[i] = Range{
			start: nodeIDString(prefix, '0'),
			end:   nodeIDString(prefix, 'f'),
		}
	}

	return out
}

func Range8() []Range {
	out := make([]Range, 8)

	for i := 0; i < 8; i++ {
		start := string([]byte{hexAlpha[i*2]})
		end := string([]byte{hexAlpha[i*2+1]})

		out[i] = Range{
			start: nodeIDString(start, '0'),
			end:   nodeIDString(end, 'f'),
		}
	}

	return out
}

func Range4() []Range {
	out := make([]Range, 4)

	for i := 0; i < 4; i++ {
		start := string([]byte{hexAlpha[i*4]})
		end := string([]byte{hexAlpha[i*4+3]})

		out[i] = Range{
			start: nodeIDString(start, '0'),
			end:   nodeIDString(end, 'f'),
		}
	}

	return out
}

func (c *CrawlerV2) StartDaemon() error {
	ch := make(chan common.NodeJSON, 64)

	for _, v := range Range4() {
		c.wg.Add(1)
		go c.sliceCrawler(v.start, v.end, ch)
	}

	c.wg.Add(1)
	go c.updaterLoop(ch)

	return nil
}

func (c *CrawlerV2) updaterLoop(ch <-chan common.NodeJSON) {
	c.wg.Done()

	for {
		node := <-ch

		err := c.db.UpsertCrawledNode(node)
		if err != nil {
			log.Error("upsert crawled node failed", "err", err, "node_id", node.ID())
		}
	}
}

func (c *CrawlerV2) crawlNode(node *enode.Node, ch chan<- common.NodeJSON) {
	nodeJSON := common.NodeJSON{
		N:       node,
		EthNode: true,
	}

	clientInfo, err := crawler.GetClientInfo(c.nodeKey, c.genesis, c.networkID, "", node)
	if err != nil {
		if errors.Is(err, crawler.ErrNotEthNode) {
			nodeJSON.EthNode = false
			ch <- nodeJSON

			return
		}

		e := err.Error()
		if strings.Contains(e, "too many peers") {
			nodeJSON.Error = "too many peers"
		} else if strings.Contains(e, "connection reset by peer") {
			nodeJSON.Error = "connection reset by peer"
		} else if strings.Contains(e, "i/o timeout") {
			nodeJSON.Error = "i/o timeout"
		} else if strings.Contains(e, "connection refused") {
			nodeJSON.Error = "connection refused"
		} else if strings.Contains(e, "EOF") {
			nodeJSON.Error = "EOF"
		} else if strings.Contains(e, "disconnect requested") {
			nodeJSON.Error = "disconnect requested"
		} else {
			log.Info("get client info failed", "node", node.ID().TerminalString(), "err", err)
			nodeJSON.Error = e
		}
	}

	nodeJSON.Info = clientInfo

	ch <- nodeJSON

}

func (c *CrawlerV2) sliceCrawler(nIDStart string, nIDEnd string, ch chan<- common.NodeJSON) {
	defer c.wg.Done()

	log.Info("start crawler", "start", nIDStart, "end", nIDEnd)

	for {
		nodes, err := c.db.SelectDiscoveredNodeSlice(nIDStart, nIDEnd, 100)
		if err != nil {
			log.Error("selecting discovered node slice failed", "err", err)
			time.Sleep(time.Minute)

			continue
		}

		if len(nodes) == 0 {
			log.Info("no nodes to crawl", "start", nIDStart, "end", nIDEnd)
			time.Sleep(time.Minute)

			continue
		}

		for _, node := range nodes {
			c.crawlNode(node, ch)
		}

		// Wait for database updater to catch up a bit
		time.Sleep(time.Minute)
	}
}
