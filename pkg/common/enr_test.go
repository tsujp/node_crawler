package common

import (
	"crypto/ecdsa"
	"net"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
)

var keys = []*ecdsa.PrivateKey{
	genKey(),
	genKey(),
}

func genKey() *ecdsa.PrivateKey {
	key, err := crypto.GenerateKey()
	if err != nil {
		panic(err)
	}

	return key
}

func newENR(key *ecdsa.PrivateKey, seq uint64) *enr.Record {
	var r enr.Record

	r.Set(enr.IP(net.IPv4(127, 0, 0, 1)))
	r.Set(enr.UDP(30303))
	r.Set(enr.TCP(30303))
	r.SetSeq(seq)

	err := enode.SignV4(&r, key)
	if err != nil {
		panic(err)
	}

	return &r
}

func enodeRecord(r *enr.Record) *enr.Record {
	return RecordToEnodeV4(r).Record()
}

func TestBestENR(t *testing.T) {
	t.Run("enode enode", func(t *testing.T) {
		a := enodeRecord(newENR(keys[0], 42))
		b := enodeRecord(newENR(keys[1], 42))
		best := BestRecord(a, b)

		if best != b {
			t.Error("best is not b")
		}
	})

	t.Run("enr enode", func(t *testing.T) {
		a := newENR(keys[0], 42)
		b := enodeRecord(newENR(keys[1], 0))
		best := BestRecord(a, b)

		if best != a {
			t.Error("best is not a")
		}
	})

	t.Run("enr nil", func(t *testing.T) {
		a := newENR(keys[0], 42)
		best := BestRecord(a, nil)

		if best != a {
			t.Error("best is not a")
		}
	})

	t.Run("nil enr", func(t *testing.T) {
		b := newENR(keys[0], 42)
		best := BestRecord(nil, b)

		if best != b {
			t.Error("best is not b")
		}
	})

	t.Run("nil nil", func(t *testing.T) {
		best := BestRecord(nil, nil)

		if best != nil {
			t.Error("best is not nil")
		}
	})

	t.Run("enr enr", func(t *testing.T) {
		a := newENR(keys[0], 73)
		b := newENR(keys[1], 42)
		best := BestRecord(a, b)

		if best != a {
			t.Error("best is not a")
		}
	})

	t.Run("equal enr", func(t *testing.T) {
		a := newENR(keys[0], 73)
		b := newENR(keys[0], 73)
		best := BestRecord(a, b)

		if best != b {
			t.Error("best is not b")
		}
	})
}
