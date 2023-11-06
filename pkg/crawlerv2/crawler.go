package crawlerv2

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/eth/protocols/eth"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/netutil"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/crawler"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/fifomemory"
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
	workers    int

	toCrawl  chan *enode.Node
	ch       chan common.NodeJSON
	wg       *sync.WaitGroup
	listener net.Listener
}

func NewCrawlerV2(
	db *database.DB,
	nodeKey *ecdsa.PrivateKey,
	listenAddr string,
	workers int,
) (*CrawlerV2, error) {
	c := &CrawlerV2{
		db:         db,
		nodeKey:    nodeKey,
		listenAddr: listenAddr,
		workers:    workers,
	}

	c.wg = new(sync.WaitGroup)
	c.ch = make(chan common.NodeJSON, 64)
	c.toCrawl = make(chan *enode.Node)

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

	gotStatus := false
	gotBlocks := 0
	getBlocks := 1

	for {
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

				goto loopExit
			}
		case *crawler.Status:
			gotStatus = true

			nodeJSON.Info.ForkID = msg.ForkID
			nodeJSON.Info.HeadHash = msg.Head
			nodeJSON.Info.NetworkID = msg.NetworkID

			_ = conn.Write(crawler.Status{
				ProtocolVersion: msg.ProtocolVersion,
				NetworkID:       msg.NetworkID,
				TD:              msg.TD,
				Head:            msg.Genesis,
				Genesis:         msg.Genesis,
				ForkID:          msg.ForkID,
			})

			getBlock, err := c.db.GetMissingBlock(msg.NetworkID)
			if err != nil {
				log.Error("could not get missing block", "err", err)
			}

			if getBlock != nil {
				getBlocks = 2

				_ = conn.Write(crawler.GetBlockHeaders{
					RequestId: 69419,
					GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{
						Origin:  eth.HashOrNumber{Hash: *getBlock},
						Amount:  1,
						Skip:    0,
						Reverse: false,
					},
				})
			}

			_ = conn.Write(crawler.GetBlockHeaders{
				RequestId: 69420, // Just a random number.
				GetBlockHeadersRequest: &eth.GetBlockHeadersRequest{
					Origin:  eth.HashOrNumber{Hash: msg.Head},
					Amount:  1,
					Skip:    0,
					Reverse: false,
				},
			})

		case *crawler.GetBlockBodies:
			_ = conn.Write(crawler.BlockBodies{
				RequestId: msg.RequestId,
			})
		case *crawler.GetBlockHeaders:
			_ = conn.Write(crawler.BlockHeaders{
				RequestId: msg.RequestId,
			})
		case *crawler.BlockHeaders:
			gotBlocks += 1

			nodeJSON.BlockHeaders = append(
				nodeJSON.BlockHeaders,
				msg.BlockHeadersRequest...,
			)

			// Only exit once we have all the number of blocks we asked for.
			if gotBlocks == getBlocks {
				_ = conn.Write(crawler.Disconnect{Reason: p2p.DiscTooManyPeers})

				goto loopExit
			}
		case *crawler.Disconnect:
			disconnect = msg

			goto loopExit
		case *crawler.Error:
			readError = msg

			goto loopExit

		// NOOP conditions
		case *crawler.NewBlock:
		case *crawler.NewBlockHashes:
		case *crawler.NewPooledTransactionHashes:
		case *crawler.Transactions:

		default:
			log.Info("message type not handled", "type", reflect.TypeOf(msg).String())
		}
	}

loopExit:

	_ = conn.Close()

	if !nodeJSON.EthNode || gotStatus {
		c.ch <- nodeJSON

		return
	}

	if disconnect != nil {
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
		common.DirectionAccept,
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
	for i := 0; i < c.workers; i++ {
		c.wg.Add(1)
		go c.crawler()
	}

	c.wg.Add(1)
	go c.nodesToCrawlDaemon(c.workers * 4)

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

	// Sometimes after crawling, the node connects again
	// immediately, so to help the database a bit, we can
	// drop this node.
	recentlyUpdated := fifomemory.New[enode.ID](64)

	for {
		metrics.NodeUpdateBacklog.Set(float64(len(c.ch)))

		node := <-c.ch
		nodeID := node.N.ID()

		if recentlyUpdated.Contains(nodeID) {
			continue
		}

		recentlyUpdated.Push(nodeID)

		err := c.db.UpsertCrawledNode(node)
		if err != nil {
			log.Error("upsert crawled node failed", "err", err, "node_id", node.TerminalString())
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
	case strings.Contains(errStr, "invalid public key"):
		return true, "invalid public key"
	case strings.Contains(errStr, "corrupt input"):
		return true, "corrupt input"
	case strings.Contains(errStr, "could not rlp decode message"):
		return true, "rlp decode"
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
			Direction: common.DirectionDial,
			Error:     errStr,
		}

		return
	}
	defer conn.Close()

	c.getClientInfo(conn, node, common.DirectionDial)
}

// Meant to be run as a goroutine
//
// Selects nodes to crawl from the database
func (c *CrawlerV2) nodesToCrawlDaemon(batchSize int) {
	defer c.wg.Done()

	// To make sure we don't crawl the same node too often.
	recentlyCrawled := fifomemory.New[enode.ID](batchSize * 2)

	for {
		nodes, err := c.db.SelectDiscoveredNodeSlice(batchSize)
		if err != nil {
			log.Error("selecting discovered node slice failed", "err", err)
			time.Sleep(time.Minute)

			continue
		}

		for _, node := range nodes {
			nodeID := node.ID()

			if !recentlyCrawled.Contains(nodeID) {
				recentlyCrawled.Push(nodeID)
				c.toCrawl <- node
			}
		}

		if len(nodes) < batchSize {
			time.Sleep(time.Minute)
		}
	}
}

// Meant to be run as a goroutine
//
// Crawls nodes from the toCrawl channel
func (c *CrawlerV2) crawler() {
	defer c.wg.Done()

	for node := range c.toCrawl {
		c.crawlNode(node)
	}
}
