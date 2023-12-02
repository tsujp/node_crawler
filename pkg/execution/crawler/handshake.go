package crawler

import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/rlpx"
	"github.com/ethereum/node-crawler/pkg/version"
)

var (
	clientName = version.ClientName("NodeCrawler")
)

func Accept(pk *ecdsa.PrivateKey, fd net.Conn) (*ecdsa.PublicKey, *Conn, error) {
	conn := new(Conn)

	conn.Conn = rlpx.NewConn(fd, nil)

	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return nil, nil, fmt.Errorf("cannot set conn deadline: %w", err)
	}

	pubKey, err := conn.Handshake(pk)
	if err != nil {
		return nil, nil, fmt.Errorf("handshake failed: %w", err)
	}

	return pubKey, conn, nil
}

// Dial attempts to Dial the given node and perform a handshake,
func Dial(pk *ecdsa.PrivateKey, n *enode.Node, timeout time.Duration) (*Conn, error) {
	var conn Conn

	fd, err := net.DialTimeout("tcp", fmt.Sprintf("[%s]:%d", n.IP(), n.TCP()), timeout)
	if err != nil {
		return nil, err
	}

	conn.Conn = rlpx.NewConn(fd, n.Pubkey())

	if err = conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return nil, fmt.Errorf("cannot set conn deadline: %w", err)
	}

	_, err = conn.Handshake(pk)
	if err != nil {
		return nil, err
	}

	return &conn, nil
}

func writeHello(conn *Conn, priv *ecdsa.PrivateKey) error {
	pub0 := crypto.FromECDSAPub(&priv.PublicKey)[1:]

	h := &Hello{
		Name:    clientName,
		Version: 5,
		Caps: []p2p.Cap{
			{Name: "eth", Version: 66},
			{Name: "eth", Version: 67},
			{Name: "eth", Version: 68},
			{Name: "snap", Version: 1},
		},
		ListenPort: 0,
		ID:         pub0,
		Rest:       nil,
	}

	conn.ourHighestProtoVersion = 68
	conn.ourHighestSnapProtoVersion = 1

	return conn.Write(h)
}
