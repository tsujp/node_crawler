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

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"
	_ "modernc.org/sqlite/lib"
)

//nolint:exhaustruct
var app = &cli.App{
	Name:        filepath.Base(os.Args[0]),
	Version:     "v0.0.1",
	HideVersion: true,
	Flags:       Flags,
	Before:      Setup,
	Commands: []*cli.Command{
		apiCommand,
		crawlerCommand,
		printEnodeCommand,
	},
}

func main() {
	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(-127)
	}
}
