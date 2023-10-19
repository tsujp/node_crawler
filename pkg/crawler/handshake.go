package crawler

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/forkid"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/rlpx"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/node-crawler/pkg/common"
)

var (
	_status          *Status
	lastStatusUpdate time.Time
	ErrNotEthNode    = errors.New("not an eth node")
)

func GetClientInfo(pk *ecdsa.PrivateKey, genesis *core.Genesis, networkID uint64, nodeURL string, n *enode.Node) (*common.ClientInfo, error) {
	var info common.ClientInfo

	conn, err := Dial(pk, n)
	if err != nil {
		return nil, fmt.Errorf("dail failed: %w", err)
	}
	defer conn.Close()

	if err = conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return nil, fmt.Errorf("cannot set conn deadline: %w", err)
	}

	if err = WriteHello(conn, pk); err != nil {
		return nil, fmt.Errorf("write hello failed: %w", err)
	}
	if err = ReadHello(conn, &info); err != nil {
		return nil, fmt.Errorf("read hello failed: %w", err)
	}

	// If node provides no eth version, we can skip it.
	if conn.NegotiatedProtoVersion == 0 {
		return nil, ErrNotEthNode
	}

	if err = conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return nil, fmt.Errorf("set conn deadline failed: %w", err)
	}

	s := getStatus(genesis.Config, uint32(conn.NegotiatedProtoVersion), genesis.ToBlock(), networkID, nodeURL)
	if err = conn.Write(s); err != nil {
		return nil, fmt.Errorf("get status failed: %w", err)
	}

	// Regardless of whether we wrote a status message or not, the remote side
	// might still send us one.

	if err = readStatus(conn, &info); err != nil {
		return nil, fmt.Errorf("read status failed: %w", err)
	}

	// Disconnect from client
	_ = conn.Write(Disconnect{Reason: p2p.DiscQuitting})

	return &info, nil
}

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
func Dial(pk *ecdsa.PrivateKey, n *enode.Node) (*Conn, error) {
	var conn Conn

	fd, err := net.DialTimeout("tcp", fmt.Sprintf("[%s]:%d", n.IP(), n.TCP()), 15*time.Second)
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

func WriteHello(conn *Conn, priv *ecdsa.PrivateKey) error {
	pub0 := crypto.FromECDSAPub(&priv.PublicKey)[1:]

	h := &Hello{
		Version: 5,
		Caps: []p2p.Cap{
			{Name: "eth", Version: 66},
			{Name: "eth", Version: 67},
			{Name: "eth", Version: 68},
			{Name: "snap", Version: 1},
		},
		ID: pub0,
	}

	conn.ourHighestProtoVersion = 68
	conn.ourHighestSnapProtoVersion = 1

	return conn.Write(h)
}

func ReadHello(conn *Conn, info *common.ClientInfo) error {
	switch msg := conn.Read().(type) {
	case *Hello:
		// set snappy if version is at least 5
		if msg.Version >= 5 {
			conn.SetSnappy(true)
		}
		info.Capabilities = msg.Caps
		info.RLPxVersion = msg.Version
		info.ClientName = msg.Name

		conn.NegotiateEthProtocol(info.Capabilities)

		return nil
	case *Disconnect:
		return fmt.Errorf("bad hello handshake disconnect: %v", msg.Reason.Error())
	case *Error:
		return fmt.Errorf("bad hello handshake error: %v", msg.Error())
	default:
		return fmt.Errorf("bad hello handshake code: %v", msg.Code())
	}
}

func getStatus(config *params.ChainConfig, version uint32, genesis *ethTypes.Block, network uint64, nodeURL string) *Status {
	if _status == nil {
		_status = &Status{
			ProtocolVersion: version,
			NetworkID:       network,
			TD:              big.NewInt(0),
			Head:            genesis.Hash(),
			Genesis:         genesis.Hash(),
			ForkID:          forkid.NewID(config, genesis, 0, 0),
		}
	}

	if nodeURL != "" && time.Since(lastStatusUpdate) > 15*time.Second {
		cl, err := ethclient.Dial(nodeURL)
		if err != nil {
			log.Error("ethclient.Dial", "err", err)
			return _status
		}

		header, err := cl.HeaderByNumber(context.Background(), nil)
		if err != nil {
			log.Error("cannot get header by number", "err", err)
			return _status
		}

		_status.Head = header.Hash()
		_status.ForkID = forkid.NewID(config, genesis, header.Number.Uint64(), header.Time)
	}

	return _status
}

func readStatus(conn *Conn, info *common.ClientInfo) error {
	switch msg := conn.Read().(type) {
	case *Status:
		info.ForkID = msg.ForkID
		info.HeadHash = msg.Head
		info.NetworkID = msg.NetworkID
		// Set correct TD if received TD is higher
		if msg.TD.Cmp(_status.TD) > 0 {
			_status.TD = msg.TD
		}
	case *Ping:
		err := conn.Write(Pong{})
		if err != nil {
			return fmt.Errorf("write pong failed: %w", err)
		}

		return readStatus(conn, info)
	case *Disconnect:
		return fmt.Errorf("bad status handshake disconnect: %v", msg.Reason.Error())
	case *Error:
		return fmt.Errorf("bad status handshake error: %v", msg.Error())
	default:
		return fmt.Errorf("bad status handshake code: %v", msg.Code())
	}
	return nil
}
