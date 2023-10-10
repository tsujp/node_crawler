package crawler

import (
	"fmt"
	"net"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/node-crawler/pkg/common"
)

func (c Crawler) makeDiscoveryConfig() (*enode.LocalNode, discover.Config) {
	var cfg discover.Config
	var err error

	if c.NodeKey != nil {
		cfg.PrivateKey = c.NodeKey
	} else {
		cfg.PrivateKey, _ = crypto.GenerateKey()
	}

	cfg.Bootnodes, err = c.parseBootnodes()
	if err != nil {
		panic(err)
	}

	return enode.NewLocalNode(c.NodeDB, cfg.PrivateKey), cfg
}

func listen(ln *enode.LocalNode, addr string) *net.UDPConn {
	socket, err := net.ListenPacket("udp4", addr)
	if err != nil {
		panic(err)
	}
	usocket := socket.(*net.UDPConn)
	uaddr := socket.LocalAddr().(*net.UDPAddr)
	if uaddr.IP.IsUnspecified() {
		ln.SetFallbackIP(net.IP{127, 0, 0, 1})
	} else {
		ln.SetFallbackIP(uaddr.IP)
	}
	ln.SetFallbackUDP(uaddr.Port)
	return usocket
}

func (c Crawler) parseBootnodes() ([]*enode.Node, error) {
	bootnodes := params.MainnetBootnodes
	if len(c.Bootnodes) != 0 {
		bootnodes = c.Bootnodes
	}

	nodes := make([]*enode.Node, len(bootnodes))
	var err error
	for i, record := range bootnodes {
		nodes[i], err = common.ParseNode(record)
		if err != nil {
			return nil, fmt.Errorf("invalid bootstrap node: %v", err)
		}
	}
	return nodes, nil
}
