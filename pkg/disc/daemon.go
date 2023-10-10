package disc

import (
	"crypto/ecdsa"
	"fmt"
	"net"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/node-crawler/pkg/database"
)

type Discovery struct {
	db         *database.DiscDB
	listenAddr string

	privateKey *ecdsa.PrivateKey
	bootnodes  []*enode.Node

	nodeDB    *enode.DB
	localnode *enode.LocalNode

	v4 *discover.UDPv4
	v5 *discover.UDPv5
}

func New(
	db *database.DiscDB,
	listenAddr string,
) (*Discovery, error) {
	d := &Discovery{
		db:         db,
		listenAddr: listenAddr,
	}

	var err error

	d.privateKey, err = db.NodeKey()
	if err != nil {
		return nil, fmt.Errorf("getting node key failed: %w", err)
	}

	bootnodes := params.MainnetBootnodes
	d.bootnodes = make([]*enode.Node, len(bootnodes))

	for i, record := range bootnodes {
		d.bootnodes[i], err = enode.ParseV4(record)
		if err != nil {
			return nil, fmt.Errorf("parsing bootnode failed: %w", err)
		}
	}

	nodeDB, err := enode.OpenDB("") // In memory
	if err != nil {
		return nil, fmt.Errorf("opening enode DB failed: %w", err)
	}
	d.nodeDB = nodeDB
	d.localnode = enode.NewLocalNode(nodeDB, d.privateKey)

	return d, nil
}

func (d *Discovery) setupDiscovery() error {
	addr, err := net.ResolveUDPAddr("udp", d.listenAddr)
	if err != nil {
		return fmt.Errorf("resolving udp address failed: %w", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listening udp failed: %w", err)
	}

	unhandled := make(chan discover.ReadPacket, 128)
	sharedConn := &sharedUDPConn{conn, unhandled}

	d.v4, err = discover.ListenV4(conn, d.localnode, discover.Config{
		PrivateKey: d.privateKey,
		Bootnodes:  d.bootnodes,
		Unhandled:  unhandled,
	})
	if err != nil {
		return fmt.Errorf("setting up discv4 failed: %w", err)
	}

	d.v5, err = discover.ListenV5(sharedConn, d.localnode, discover.Config{
		PrivateKey: d.privateKey,
		Bootnodes:  d.bootnodes,
	})
	if err != nil {
		return fmt.Errorf("setting up discv5 failed: %w", err)
	}

	return nil
}

func (d *Discovery) Close() {
	d.v4.Close()
	d.v5.Close()
}

func (d *Discovery) discLoop(iter enode.Iterator, ch chan<- *enode.Node) {
	for iter.Next() {
		ch <- iter.Node()
	}
}

func (d *Discovery) updaterLoop(ch <-chan *enode.Node) {
	for {
		node := <-ch

		err := d.db.UpsertNode(node)
		if err != nil {
			log.Error("upserting node failed", "err", err)
		}
	}
}

// Starts the discovery in a goroutine
func (d *Discovery) StartDaemon() error {
	err := d.setupDiscovery()
	if err != nil {
		return fmt.Errorf("setting up discovery failed: %w", err)
	}

	ch := make(chan *enode.Node, 64)
	go d.discLoop(d.v4.RandomNodes(), ch)
	go d.discLoop(d.v5.RandomNodes(), ch)
	go d.updaterLoop(ch)

	return nil
}
