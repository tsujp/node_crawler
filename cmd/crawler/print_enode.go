package main

import (
	"fmt"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/urfave/cli/v2"
)

var (
	printEnodeCommand = &cli.Command{
		Name:      "print-enode",
		Usage:     "Print the enode given the node key file",
		ArgsUsage: "KEY_FILE HOST PORT",
		Action:    printEnodeCmd,
	}
)

func printEnodeCmd(cCtx *cli.Context) error {
	args := cCtx.Args()

	if args.Len() != 3 {
		return fmt.Errorf("3 positional arguments required")
	}

	nodeFile := args.Get(0)
	hostname := args.Get(1)
	port := args.Get(2)

	nodeKey, err := crypto.LoadECDSA(nodeFile)
	if err != nil {
		return fmt.Errorf("error reading node key file: %w", err)
	}

	fmt.Printf(
		"enode://%x@%s:%s\n",
		crypto.FromECDSAPub(&nodeKey.PublicKey)[1:],
		hostname,
		port,
	)

	return nil
}
