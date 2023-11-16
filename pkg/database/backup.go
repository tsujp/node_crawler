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

func (db *DB) backup(backupFilename string) {
	backupName := time.Now().UTC().Format(backupFilename)

	log.Info("Backup starting", "name", backupName)

	backupStart := time.Now()

	_, err := db.db.Exec(fmt.Sprintf("VACUUM INTO '%s'", backupName))
	if err != nil {
		log.Error("Backup failed", "name", backupName, "err", err)

		return
	}

	log.Info("Backup done", "name", backupName, "duration", time.Since(backupStart))

	backupFile, err := os.Open(backupName)
	if err != nil {
		log.Error("Opening backup file failed", "name", backupFile, "err", err)

		return
	}
	defer backupFile.Close()

	tmpExtension := ".tmp"
	compressedTmpName := backupName + ".gz" + tmpExtension

	compressedFile, err := os.OpenFile(
		compressedTmpName,
		os.O_CREATE|os.O_TRUNC|os.O_RDWR,
		0o600,
	)
	if err != nil {
		log.Error("Opening compressed backup failed", "name", backupName, "err", err)

		return
	}
	defer compressedFile.Close()

	writer := gzip.NewWriter(compressedFile)

	log.Info("Backup compress starting", "name", backupName)

	compressStart := time.Now()

	_, err = io.Copy(writer, backupFile)
	if err != nil {
		log.Error("Writing compressed backup failed", "name", backupName, "err", err)

		return
	}

	log.Info("Backup compress done", "name", backupName, "duration", time.Since(compressStart))

	err = backupFile.Close()
	if err != nil {
		log.Error("Backup file close failed", "name", backupName, "err", err)

		return
	}

	err = writer.Flush()
	if err != nil {
		log.Error("Backup compressed file flush failed", "name", backupName, "err", err)

		return
	}

	err = writer.Close()
	if err != nil {
		log.Error("Backup compressed writer close failed", "name", backupName, "err", err)

		return
	}

	err = compressedFile.Close()
	if err != nil {
		log.Error("Backup compressed writer close failed", "name", backupName, "err", err)

		return
	}

	err = os.Remove(backupName)
	if err != nil {
		log.Error("Backup file cleanup failed", "name", backupName, "err", err)

		return
	}

	err = os.Rename(
		compressedTmpName,
		strings.TrimSuffix(compressedTmpName, tmpExtension),
	)
	if err != nil {
		log.Error("Backup moving temp compressed file fialed", "name", backupName, "err", err)

		return
	}
}

// Meant to be run as a goroutine
//
// Takes a daily backup of the database using the backup filename.
// The backupFilename is passed to the date format function, so you can
// put a date template in there.
func (db *DB) BackupDaemon(backupFilename string) {
	dirName := path.Dir(backupFilename)

	_, err := os.Stat(dirName)

	if os.IsNotExist(err) {
		err = os.MkdirAll(dirName, 0o750)
		if err != nil {
			log.Error("Could not make backup dir", "err", err)

			return
		}
	} else if err != nil {
		log.Error("Could not read backup dir", "err", err)

		return
	}

	for {
		// Start of tomorrow
		nextBackup := today().AddDate(0, 0, 1)
		time.Sleep(time.Until(nextBackup))

		db.backup(backupFilename)
	}
}
