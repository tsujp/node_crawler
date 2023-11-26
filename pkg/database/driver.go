package database

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"

	"modernc.org/sqlite"
)

func init() {
	sqlite.RegisterConnectionHook(attachHook)
}

func attachHook(
	conn interface {
		driver.ExecerContext
		driver.QueryerContext
	}, dsn string,
) error {
	uri, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}

	query := uri.Query()

	for _, attachStr := range query["_attach"] {
		unescapedAttachStr, err := url.QueryUnescape(attachStr)
		if err != nil {
			return fmt.Errorf(
				"unescape attach filename: %s: %w", attachStr, err)
		}

		attachURI, err := url.Parse(unescapedAttachStr)
		if err != nil {
			return fmt.Errorf(
				"parse attach uri: %s: %w", unescapedAttachStr, err)
		}

		err = attachDatabase(conn, attachURI)
		if err != nil {
			return fmt.Errorf("attach database: %w", err)
		}
	}

	return nil
}

func attachDatabase(conn driver.ExecerContext, uri *url.URL) error {
	query := uri.Query()
	databaseName := query.Get("_name")

	_, err := conn.ExecContext(
		context.Background(),
		fmt.Sprintf(`ATTACH DATABASE '%s' AS %s`, uri.Path, databaseName),
		nil,
	)
	if err != nil {
		return fmt.Errorf("attach exec failed: %w", err)
	}

	for _, value := range query["_pragma"] {
		cmd := "pragma " + databaseName + "." + value

		_, err = conn.ExecContext(context.Background(), cmd, nil)
		if err != nil {
			return fmt.Errorf("pragma exec failed: %w", err)
		}
	}

	return nil
}
