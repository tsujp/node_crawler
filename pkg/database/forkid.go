package database

import (
	"encoding/binary"
	"encoding/hex"
)

type ForkID [4]byte

func (fid ForkID) Uint32() uint32 {
	return binary.BigEndian.Uint32(fid[:])
}

func (fid ForkID) Hex() string {
	return hex.EncodeToString(fid[:])
}

func (fid ForkID) String() string {
	return fid.Hex()
}

func Uint32ToForkID(i uint32) ForkID {
	fid := ForkID{}
	binary.BigEndian.PutUint32(fid[:], i)

	return fid
}
