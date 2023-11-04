package database

import (
	"encoding/binary"
	"hash/crc32"
	"math/big"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

type ForkMaps struct {
	Hash      map[uint32]string
	BlockTime map[uint64]string
}

var Forks = map[int64]ForkMaps{
	params.MainnetChainConfig.ChainID.Int64(): createForkMap(
		core.DefaultGenesisBlock().ToBlock(),
		params.MainnetChainConfig,
	),
	params.GoerliChainConfig.ChainID.Int64(): createForkMap(
		core.DefaultGoerliGenesisBlock().ToBlock(),
		params.GoerliChainConfig,
	),
	params.SepoliaChainConfig.ChainID.Int64(): createForkMap(
		core.DefaultSepoliaGenesisBlock().ToBlock(),
		params.SepoliaChainConfig,
	),
	params.HoleskyChainConfig.ChainID.Int64(): createForkMap(
		core.DefaultHoleskyGenesisBlock().ToBlock(),
		params.HoleskyChainConfig,
	),
}

var (
	ForkNameHomeStead      = "Homestead"
	ForkNameDAO            = "DAO Fork"
	ForkNameEIP150         = "Tangerine Whistle (EIP 150)"
	ForkNameEIP155         = "Spurious Dragon/1 (EIP 155)"
	ForkNameEIP158         = "Spurious Dragon/2 (EIP 158)"
	ForkNameByzantium      = "Byzantium"
	ForkNameConstantinople = "Constantinople"
	ForkNamePetersburg     = "Petersburg"
	ForkNameIstanbul       = "Istanbul"
	ForkNameMuirGlacier    = "Muir Glacier"
	ForkNameBerlin         = "Berlin"
	ForkNameLondon         = "London"
	ForkNameArrowGlacier   = "Arrow Glacier"
	ForkNameGrayGlacier    = "Gray Glacier"
	ForkNameMerge          = "Merge"
	ForkNameShanghai       = "Shanghai"
	ForkNameCancun         = "Cancun"
	ForkNamePrague         = "Prague"
	ForkNameVerkle         = "Verkle"
)

var ForkNames = []string{
	ForkNameHomeStead,
	ForkNameDAO,
	ForkNameEIP150,
	ForkNameEIP155,
	ForkNameEIP158,
	ForkNameByzantium,
	ForkNameConstantinople,
	ForkNamePetersburg,
	ForkNameIstanbul,
	ForkNameMuirGlacier,
	ForkNameBerlin,
	ForkNameLondon,
	ForkNameArrowGlacier,
	ForkNameGrayGlacier,
	ForkNameMerge,
	ForkNameShanghai,
	ForkNameCancun,
	ForkNamePrague,
	ForkNameVerkle,
}

var Chains = map[int64]*params.ChainConfig{
	params.MainnetChainConfig.ChainID.Int64(): params.MainnetChainConfig,
	params.GoerliChainConfig.ChainID.Int64():  params.GoerliChainConfig,
	params.SepoliaChainConfig.ChainID.Int64(): params.SepoliaChainConfig,
	params.HoleskyChainConfig.ChainID.Int64(): params.HoleskyChainConfig,
}

func checksumUpdate(hash uint32, fork uint64) uint32 {
	var blob [8]byte
	binary.BigEndian.PutUint64(blob[:], fork)

	return crc32.Update(hash, crc32.IEEETable, blob[:])
}

func uint64prt(i *big.Int) *uint64 {
	if i == nil {
		return nil
	}

	ui64 := i.Uint64()
	return &ui64
}

func createForkMap(genesis *types.Block, config *params.ChainConfig) ForkMaps {
	currentHash := crc32.ChecksumIEEE(genesis.Hash().Bytes())

	out := ForkMaps{
		Hash: map[uint32]string{
			currentHash: "Genesis",
		},
		BlockTime: map[uint64]string{},
	}

	blocks := []*uint64{
		uint64prt(config.HomesteadBlock),
		uint64prt(config.DAOForkBlock),
		uint64prt(config.EIP150Block),
		uint64prt(config.EIP155Block),
		uint64prt(config.EIP158Block),
		uint64prt(config.ByzantiumBlock),
		uint64prt(config.ConstantinopleBlock),
		uint64prt(config.PetersburgBlock),
		uint64prt(config.IstanbulBlock),
		uint64prt(config.MuirGlacierBlock),
		uint64prt(config.BerlinBlock),
		uint64prt(config.LondonBlock),
		uint64prt(config.ArrowGlacierBlock),
		uint64prt(config.GrayGlacierBlock),
		uint64prt(config.MergeNetsplitBlock),
	}

	times := []*uint64{
		config.ShanghaiTime,
		config.CancunTime,
		config.PragueTime,
		config.VerkleTime,
	}

	var previousBlock uint64 = 0

	for i, block := range blocks {
		// Deduplicate blocks in the same fork
		if block != nil && previousBlock != *block {
			currentHash = checksumUpdate(currentHash, *block)

			name := ForkNames[i]
			out.Hash[currentHash] = name
			out.BlockTime[*block] = name

			previousBlock = *block
		}
	}

	var previousBlockTime uint64 = 0

	for i, blockTime := range times {
		// Deduplicate blocks in the same fork
		if blockTime != nil && previousBlockTime != *blockTime && *blockTime > genesis.Time() {
			currentHash = checksumUpdate(currentHash, *blockTime)

			name := ForkNames[i+len(blocks)]
			out.Hash[currentHash] = name
			out.BlockTime[*blockTime] = name

			previousBlockTime = *blockTime
		}
	}

	return out
}
