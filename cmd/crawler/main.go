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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/urfave/cli/v2"
	_ "modernc.org/sqlite/lib"
)

var (
	app = &cli.App{
		Name:        filepath.Base(os.Args[0]),
		Usage:       "go-ethereum crawler",
		Version:     "v.0.0.1",
		Writer:      os.Stdout,
		HideVersion: true,
	}
)

func init() {
	app.Flags = append(app.Flags, Flags...)
	app.Before = func(ctx *cli.Context) error {
		return Setup(ctx)
	}
	// Add subcommands.
	app.Commands = []*cli.Command{
		apiCommand,
		crawlerCommand,
		printEnodeCommand,
		{
			Name: "migrate-db",
			Action: func(ctx *cli.Context) error {
				name := ctx.Args().Get(0)

				sqlite, err := sql.Open("sqlite", name)
				if err != nil {
					return fmt.Errorf("open new db failed: %w", err)
				}

				db := database.NewDB(sqlite, nil, time.Second, time.Second, time.Second)

				// err = db.CreateTables()
				// if err != nil {
				// 	return fmt.Errorf("create tables failed: %w", err)
				// }

				err = db.Migrate()
				if err != nil {
					return fmt.Errorf("migrate failed: %w", err)
				}

				return nil
			},
		},
	}
}

func main() {
	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(-127)
	}
}
