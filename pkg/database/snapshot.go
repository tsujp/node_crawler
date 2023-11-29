package database

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

func today() time.Time {
	year, month, day := time.Now().UTC().Date()

	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func (db *DB) snapshot(database string, snapshotFilename string) {
	snapshotName := time.Now().UTC().Format(snapshotFilename)

	log.Info(
		"Snapshot starting",
		"database", database,
		"name", snapshotName,
	)

	snapshotStart := time.Now()

	_, err := db.ExecRetryBusy(fmt.Sprintf("VACUUM %s INTO '%s'", database, snapshotName))
	if err != nil {
		log.Error(
			"Snapshot failed",
			"database", database,
			"name", snapshotName,
			"err", err,
		)

		return
	}

	log.Info(
		"Snapshot done",
		"database", database,
		"name", snapshotName,
		"duration", time.Since(snapshotStart),
	)

	snapshotFile, err := os.Open(snapshotName)
	if err != nil {
		log.Error(
			"Opening snapshot file failed",
			"database", database,
			"name", snapshotFile,
			"err", err,
		)

		return
	}
	defer snapshotFile.Close()

	tmpExtension := ".tmp"
	compressedTmpName := snapshotName + ".gz" + tmpExtension

	compressedFile, err := os.OpenFile(
		compressedTmpName,
		os.O_CREATE|os.O_TRUNC|os.O_RDWR,
		0o600,
	)
	if err != nil {
		log.Error(
			"Opening compressed snapshot failed",
			"database", database,
			"name", snapshotName,
			"err", err,
		)

		return
	}
	defer compressedFile.Close()

	writer := gzip.NewWriter(compressedFile)

	log.Info(
		"Snapshot compress starting",
		"database", database,
		"name", snapshotName,
	)

	compressStart := time.Now()

	_, err = io.Copy(writer, snapshotFile)
	if err != nil {
		log.Error(
			"Writing compressed snapshot failed",
			"database", database,
			"name", snapshotName,
			"err", err,
		)

		return
	}

	log.Info(
		"Snapshot compress done",
		"database", database,
		"name", snapshotName,
		"duration", time.Since(compressStart),
	)

	err = snapshotFile.Close()
	if err != nil {
		log.Error(
			"Snapshot file close failed",
			"database", database,
			"name", snapshotName,
			"err", err,
		)

		return
	}

	err = writer.Flush()
	if err != nil {
		log.Error(
			"Snapshot compressed file flush failed",
			"database", database,
			"name", snapshotName,
			"err", err,
		)

		return
	}

	err = writer.Close()
	if err != nil {
		log.Error(
			"Snapshot compressed writer close failed",
			"database", database,
			"name", snapshotName,
			"err", err,
		)

		return
	}

	err = compressedFile.Close()
	if err != nil {
		log.Error(
			"Snapshot compressed writer close failed",
			"database", database,
			"name", snapshotName,
			"err", err,
		)

		return
	}

	err = os.Remove(snapshotName)
	if err != nil {
		log.Error(
			"Snpashot file cleanup failed",
			"database", database,
			"name", snapshotName,
			"err", err,
		)

		return
	}

	err = os.Rename(
		compressedTmpName,
		strings.TrimSuffix(compressedTmpName, tmpExtension),
	)
	if err != nil {
		log.Error(
			"Snapshot moving temp compressed file fialed",
			"database", database,
			"name", snapshotName,
			"err", err,
		)

		return
	}
}

// Meant to be run as a goroutine
//
// Takes a daily snapshot of the database using the snapshot filename.
// The snapshotFilename is passed to the date format function, so you can
// put a date template in there.
func (db *DB) SnapshotDaemon(database string, snapshotDir string, snapshotFilename string) {
	_, err := os.Stat(snapshotDir)

	if os.IsNotExist(err) {
		err = os.MkdirAll(snapshotDir, 0o750)
		if err != nil {
			log.Error("Could not make snapshot dir", "err", err)

			return
		}
	} else if err != nil {
		log.Error("Could not read snapshot dir", "err", err)

		return
	}

	fullName := path.Join(snapshotDir, snapshotFilename)

	for {
		// Start of tomorrow
		nextSnapshot := today().AddDate(0, 0, 1)
		time.Sleep(time.Until(nextSnapshot))

		db.snapshot(database, fullName)
	}
}
