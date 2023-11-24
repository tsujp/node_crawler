// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package common

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
)

func IsEnodeV4(source string) bool {
	return strings.HasPrefix(source, "enode://")
}

// ParseNode parses a node record and verifies its signature.
func ParseNode(source string) (*enode.Node, error) {
	if IsEnodeV4(source) {
		return enode.ParseV4(source)
	}

	r, err := parseRecord(source)
	if err != nil {
		return nil, err
	}

	return enode.New(enode.ValidSchemes, r)
}

func EncodeENR(r *enr.Record) []byte {
	// Always succeeds because record is valid.
	nodeRecord, _ := rlp.EncodeToBytes(r)

	return nodeRecord
}

func LoadENR(b []byte) (*enr.Record, error) {
	var record enr.Record

	err := rlp.DecodeBytes(b, &record)
	if err != nil {
		return nil, fmt.Errorf("decode bytes failed: %w", err)
	}

	return &record, nil
}

func ENRString(r *enr.Record) string {
	// Always succeeds because record is valid.
	enc, _ := rlp.EncodeToBytes(r)
	b64 := base64.RawURLEncoding.EncodeToString(enc)

	return "enr:" + b64
}

func RecordIP(r *enr.Record) net.IP {
	var (
		ip4 enr.IPv4
		ip6 enr.IPv6
	)

	if r.Load(&ip4) == nil {
		return net.IP(ip4)
	}

	if r.Load(&ip6) == nil {
		return net.IP(ip6)
	}

	return nil
}

func RecordToV4(r *enr.Record) *enode.Node {
	ip := RecordIP(r)

	var tcp enr.TCP
	_ = r.Load(&tcp)

	var udp enr.UDP
	_ = r.Load(&udp)

	var pubkey enode.Secp256k1

	_ = r.Load(&pubkey)

	pkey := ecdsa.PublicKey(pubkey)

	return enode.NewV4(&pkey, ip, int(tcp), int(udp))

}

func RecordToEnode(r *enr.Record) (*enode.Node, error) {
	if r.IdentityScheme() == "" {
		return RecordToV4(r), nil
	}

	return enode.New(enode.ValidSchemes, r)
}

func EnodeString(r *enr.Record) string {
	node, err := RecordToEnode(r)
	if err != nil {
		log.Error("enode to string failed", "err", err)
		return ""
	}

	return node.URLv4()
}

// parseRecord parses a node record from hex, base64, or raw binary input.
func parseRecord(source string) (*enr.Record, error) {
	bin := []byte(source)
	if d, ok := decodeRecordHex(bytes.TrimSpace(bin)); ok {
		bin = d
	} else if d, ok := decodeRecordBase64(bytes.TrimSpace(bin)); ok {
		bin = d
	}
	var r enr.Record
	err := rlp.DecodeBytes(bin, &r)
	return &r, err
}

func decodeRecordHex(b []byte) ([]byte, bool) {
	if bytes.HasPrefix(b, []byte("0x")) {
		b = b[2:]
	}
	dec := make([]byte, hex.DecodedLen(len(b)))
	_, err := hex.Decode(dec, b)
	return dec, err == nil
}

func decodeRecordBase64(b []byte) ([]byte, bool) {
	if bytes.HasPrefix(b, []byte("enr:")) {
		b = b[4:]
	}
	dec := make([]byte, base64.RawURLEncoding.DecodedLen(len(b)))
	n, err := base64.RawURLEncoding.Decode(dec, b)
	return dec[:n], err == nil
}

// attrFormatters contains formatting functions for well-known ENR keys.
var attrFormatters = map[string]func(rlp.RawValue) (string, bool){
	"id":   formatAttrString,
	"ip":   formatAttrIP,
	"ip6":  formatAttrIP,
	"tcp":  formatAttrUint,
	"tcp6": formatAttrUint,
	"udp":  formatAttrUint,
	"udp6": formatAttrUint,
}

func formatAttrRaw(v rlp.RawValue) (string, bool) {
	s := hex.EncodeToString(v)
	return s, true
}

func formatAttrString(v rlp.RawValue) (string, bool) {
	content, _, err := rlp.SplitString(v)
	return strconv.Quote(string(content)), err == nil
}

func formatAttrIP(v rlp.RawValue) (string, bool) {
	content, _, err := rlp.SplitString(v)
	if err != nil || len(content) != 4 && len(content) != 6 {
		return "", false
	}
	return net.IP(content).String(), true
}

func formatAttrUint(v rlp.RawValue) (string, bool) {
	var x uint64
	if err := rlp.DecodeBytes(v, &x); err != nil {
		return "", false
	}
	return strconv.FormatUint(x, 10), true
}
