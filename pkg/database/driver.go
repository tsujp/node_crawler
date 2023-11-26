package database

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"

	"modernc.org/sqlite"
)

type Driver struct {
	sqlite.Driver
}

func driverConnExec(conn driver.Conn, cmd string, args []driver.Value) (driver.Result, error) {
	stmt, err := conn.Prepare(cmd)
	if err != nil {
		return nil, fmt.Errorf("prepare failed: %w", err)
	}
	defer stmt.Close()

	//nolint
	result, err := stmt.Exec(args)
	if err != nil {
		return nil, fmt.Errorf("exec failed: %w", err)
	}

	return result, nil
}

func attachDatabase(conn driver.Conn, uri *url.URL) error {
	query := uri.Query()
	databaseName := query.Get("_name")

	_, err := driverConnExec(
		conn,
		fmt.Sprintf(`ATTACH DATABASE '%s' AS %s`, uri.Path, databaseName),
		nil,
	)
	if err != nil {
		return fmt.Errorf("attach exec failed: %w", err)
	}

	for _, value := range query["_pragma"] {
		cmd := "pragma " + databaseName + "." + value

		_, err = driverConnExec(conn, cmd, nil)
		if err != nil {
			return fmt.Errorf("pragma exec failed: %w", err)
		}
	}

	return nil
}

func (d Driver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}

	uri, err := url.Parse(name)
	if err != nil {
		return nil, fmt.Errorf("uri parse failed: %w", err)
	}

	query := uri.Query()

	if query.Has("_attach") {
		for _, attachStr := range query["_attach"] {
			unescapedAttachStr, err := url.QueryUnescape(attachStr)
			if err != nil {
				return nil, fmt.Errorf(
					"unescape attach filename failed: %s: %w", attachStr, err)
			}

			attachURI, err := url.Parse(unescapedAttachStr)
			if err != nil {
				return nil, fmt.Errorf(
					"parse attach uri: %s failed: %w", unescapedAttachStr, err)
			}

			err = attachDatabase(conn, attachURI)
			if err != nil {
				return nil, fmt.Errorf("attach database failed: %w", err)
			}
		}
	}

	return conn, nil
}

func init() {
	sql.Register("sqlite_attach", Driver{sqlite.Driver{}})
}
