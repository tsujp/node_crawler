package crawlerv2

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/crawler"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/metrics"
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
	db         *database.DB
	nodeKey    *ecdsa.PrivateKey
	listenAddr string
	workers    uint64

	ch              chan common.NodeJSON
	wg              *sync.WaitGroup
	listener        net.Listener
	status          *crawler.Status
	totalDifficulty big.Int
	genesisBlock    *types.Block
}

func NewCrawlerV2(
	db *database.DB,
	nodeKey *ecdsa.PrivateKey,
	genesis *core.Genesis,
	networkID uint64,
	listenAddr string,
	workers uint64,
) (*CrawlerV2, error) {
	c := &CrawlerV2{
		db:         db,
		nodeKey:    nodeKey,
		listenAddr: listenAddr,
		workers:    workers,
	}

	switch workers {
	case 1, 2, 4, 8, 16, 32:
	default:
		return nil, fmt.Errorf("num crawlers: %d not in 1,2,4,8,16,32", workers)
	}

	c.wg = new(sync.WaitGroup)
	c.ch = make(chan common.NodeJSON, 64)

	td := big.NewInt(0)
	// Merge total difficulty
	td.SetString("58750003716598360000000", 10)

	genesisBlock := genesis.ToBlock()
	genesisHash := genesisBlock.Hash()

	c.status = &crawler.Status{
		ProtocolVersion: 66,
		NetworkID:       networkID,
		TD:              td,
		Head:            genesisHash,
		Genesis:         genesisHash,
		ForkID:          forkid.NewID(genesis.Config, genesisBlock, 0, 0),
	}

	return c, nil
}

func (c *CrawlerV2) Wait() {
	c.wg.Wait()
}

func (c *CrawlerV2) Close() {
	if c.listener != nil {
		c.listener.Close()
	}
}

func nodeFromConn(pubkey *ecdsa.PublicKey, conn net.Conn) *enode.Node {
	var ip net.IP
	var port int

	if tcp, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		ip = tcp.IP
		port = tcp.Port
	}

	return enode.NewV4(pubkey, ip, port, port)
}

func (c *CrawlerV2) getClientInfo(
	conn *crawler.Conn,
	node *enode.Node,
	direction string,
) {
	err := crawler.WriteHello(conn, c.nodeKey)
	if err != nil {
		known, errStr := translateError(err)
		if !known {
			log.Info("write hello failed", "err", err)
		}

		c.ch <- common.NodeJSON{
			N:         node,
			EthNode:   true,
			Direction: direction,
			Error:     errStr,
		}

		return
	}

	var disconnect *crawler.Disconnect = nil
	var readError *crawler.Error = nil

	nodeJSON := common.NodeJSON{
		N:         node,
		EthNode:   true,
		Info:      &common.ClientInfo{},
		Direction: direction,
	}

	loop := true
	for loop {
		switch msg := conn.Read().(type) {
		case *crawler.Ping:
			_ = conn.Write(crawler.Pong{})
		case *crawler.Pong:
			continue
		case *crawler.Hello:
			if msg.Version >= 5 {
				conn.SetSnappy(true)
			}
			nodeJSON.Info.Capabilities = msg.Caps
			nodeJSON.Info.RLPxVersion = msg.Version
			nodeJSON.Info.ClientName = msg.Name

			conn.NegotiateEthProtocol(nodeJSON.Info.Capabilities)

			if conn.NegotiatedProtoVersion == 0 {
				nodeJSON.Error = "not eth node"
				nodeJSON.EthNode = false
				_ = conn.Write(crawler.Disconnect{Reason: p2p.DiscUselessPeer})

				loop = false

				break
			}

			_ = conn.Write(c.status)
		case *crawler.Status:
			nodeJSON.Info.ForkID = msg.ForkID
			nodeJSON.Info.HeadHash = msg.Head
			nodeJSON.Info.NetworkID = msg.NetworkID

			_ = conn.Write(crawler.Disconnect{Reason: p2p.DiscQuitting})

			loop = false
		case *crawler.Disconnect:
			disconnect = msg
			loop = false
		case *crawler.Error:
			readError = msg
			loop = false
		default:
			log.Info("message type not handled", "msg", msg)
		}
	}

	if !nodeJSON.EthNode {
		c.ch <- nodeJSON

		return
	} else if disconnect != nil {
		nodeJSON.Error = disconnect.Reason.String()
	} else if readError != nil {
		known, errStr := translateError(readError)
		if !known {
			log.Info("message read error", "err", readError)
		}

		nodeJSON.Error = errStr
	}

	c.ch <- nodeJSON
}

func (c *CrawlerV2) crawlPeer(fd net.Conn) {
	pubKey, conn, err := crawler.Accept(c.nodeKey, fd)
	if err != nil {
		known, _ := translateError(err)
		if !known {
			log.Info("accept peer failed", "err", err, "ip", fd.RemoteAddr().String())
		}

		return
	}
	defer conn.Close()

	c.getClientInfo(
		conn,
		nodeFromConn(pubKey, fd),
		"accept",
	)
}

func (c *CrawlerV2) listenLoop() {
	defer c.wg.Done()

	for {
		var (
			conn net.Conn
			err  error
		)

		for {
			conn, err = c.listener.Accept()
			if netutil.IsTemporaryError(err) {
				time.Sleep(100 * time.Millisecond)
				continue
			} else if err != nil {
				log.Error("crawler listener accept failed", "err", err)
			}

			break
		}

		go c.crawlPeer(conn)
	}
}

func (c *CrawlerV2) startListener() error {
	listener, err := net.Listen("tcp", c.listenAddr)
	if err != nil {
		return fmt.Errorf("crawler listen failed: %w", err)
	}

	c.listener = listener

	c.wg.Add(1)
	go c.listenLoop()

	return nil
}

func (c *CrawlerV2) StartDaemon() error {
	for _, v := range rangeN(c.workers) {
		c.wg.Add(1)
		go c.sliceCrawler(v.start, v.end)
	}

	err := c.startListener()
	if err != nil {
		return fmt.Errorf("starting listener failed: %w", err)
	}

	c.wg.Add(1)
	go c.updaterLoop()

	return nil
}

func (c *CrawlerV2) updaterLoop() {
	c.wg.Done()

	for {
		metrics.NodeUpdateBacklog.Set(float64(len(c.ch)))
		node := <-c.ch

		err := c.db.UpsertCrawledNode(node)
		if err != nil {
			log.Error("upsert crawled node failed", "err", err, "node_id", node.ID())
		}

		metrics.NodeUpdateInc(node.Direction, node.Error)
	}
}

func translateError(err error) (bool, string) {
	switch errStr := err.Error(); {
	case strings.Contains(errStr, "i/o timeout"):
		return true, "i/o timeout"
	case strings.Contains(errStr, "connection reset by peer"):
		return true, "connection reset by peer"
	case strings.Contains(errStr, "EOF"):
		return true, "EOF"
	case strings.Contains(errStr, "no route to host"):
		return true, "no route to host"
	case strings.Contains(errStr, "connection refused"):
		return true, "connection refused"
	case strings.Contains(errStr, "network is unreachable"):
		return true, "network is unreachable"
	case strings.Contains(errStr, "invalid message"):
		return true, "invalid message"
	default:
		return false, errStr
	}
}

func (c *CrawlerV2) crawlNode(node *enode.Node) {
	conn, err := crawler.Dial(c.nodeKey, node, 10*time.Second)
	if err != nil {
		known, errStr := translateError(err)
		if !known {
			log.Info("dial failed", "err", err)
		}

		c.ch <- common.NodeJSON{
			N:         node,
			EthNode:   true,
			Direction: "dial",
			Error:     errStr,
		}

		return
	}
	defer conn.Close()

	c.getClientInfo(conn, node, "dial")
}

func (c *CrawlerV2) sliceCrawler(nIDStart string, nIDEnd string) {
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
			c.crawlNode(node)
		}

		// Wait for database updater to catch up a bit
		time.Sleep(10 * time.Second)
	}
}
