package edfparser

import (
	"database/sql"
	"fmt"
	"io"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func initializeDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("error opening SQLite database: %w", err)
	}

	// Apply PRAGMA settings for better performance
	_, err = db.Exec("PRAGMA synchronous = OFF")
	if err != nil {
		return nil, fmt.Errorf("error applying PRAGMA synchronous: %w", err)
	}
	_, err = db.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		return nil, fmt.Errorf("error applying PRAGMA journal_mode: %w", err)
	}

	// Create tables for Header, Signals, and Samples
	createTableQueries := []string{
		`CREATE TABLE IF NOT EXISTS header (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version TEXT,
            patient_id TEXT,
            recording_id TEXT,
            start_date TEXT,
            start_time TEXT,
            reserved TEXT,
            header_bytes INTEGER,
            num_records INTEGER,
            record_duration REAL,
            num_signals INTEGER
        );`,
		`CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT,
            transducer TEXT,
            units TEXT,
            physical_min REAL,
            physical_max REAL,
            digital_min INTEGER,
            digital_max INTEGER,
            prefiltering TEXT,
            num_samples INTEGER,
            reserved TEXT,
            header_id INTEGER,
            FOREIGN KEY(header_id) REFERENCES header(id)
        );`,
		`CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER,
            sample REAL,
            FOREIGN KEY(signal_id) REFERENCES signals(id)
        );`,
	}

	for _, query := range createTableQueries {
		_, err := db.Exec(query)
		if err != nil {
			return nil, fmt.Errorf("error creating tables: %w", err)
		}
	}

	return db, nil
}

func storeHeader(db *sql.DB, header Header) (int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	query := `INSERT INTO header (version, patient_id, recording_id, start_date, start_time, reserved, header_bytes, num_records, record_duration, num_signals)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	result, err := tx.Exec(query, header.Version, header.PatientID, header.RecordingID, header.StartDate, header.StartTime, header.Reserved, header.HeaderBytes, header.NRecords, header.RecordDuration, header.NSignals)
	if err != nil {
		return 0, fmt.Errorf("error inserting header: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("error committing transaction: %w", err)
	}

	return result.LastInsertId()
}

func storeSignals(db *sql.DB, headerID int64, signals []Signal) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO signals (label, transducer, units, physical_min, physical_max, digital_min, digital_max, prefiltering, num_samples, reserved, header_id)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("error preparing insert statement: %w", err)
	}
	defer stmt.Close()

	for _, signal := range signals {
		_, err := stmt.Exec(signal.Label, signal.Transducer, signal.Units, signal.PhysicalMin, signal.PhysicalMax, signal.DigitalMin, signal.DigitalMax, signal.Prefiltering, signal.NSamples, signal.Reserved, headerID)
		if err != nil {
			return fmt.Errorf("error inserting signal: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

func storeSamples(db *sql.DB, signalID int64, samples []float64) error {
	for _, sample := range samples {
		query := `INSERT INTO samples (signal_id, sample) VALUES (?, ?)`
		_, err := db.Exec(query, signalID, sample)
		if err != nil {
			return fmt.Errorf("error inserting sample: %w", err)
		}
	}
	return nil
}

func ProcessRecords(inputFile, outputFile *os.File, header Header, signals []Signal, scalings []ScalingFactors, db *sql.DB) error {
	bytesPerRecord := calculateBytesPerRecord(signals)
	recordBuffer := make([]byte, bytesPerRecord)

	// Store header in DB
	headerID, err := storeHeader(db, header)
	if err != nil {
		return fmt.Errorf("error storing header: %w", err)
	}

	// Store signals in DB
	err = storeSignals(db, headerID, signals)
	if err != nil {
		return fmt.Errorf("error storing signals: %w", err)
	}

	for rec := 0; rec < header.NRecords; rec++ {
		if _, err := io.ReadFull(inputFile, recordBuffer); err != nil {
			return fmt.Errorf("error reading record %d: %w", rec, err)
		}

		recordData, err := parseRecord(recordBuffer, signals, scalings)
		if err != nil {
			return fmt.Errorf("error parsing record %d: %w", rec, err)
		}

		for sigIdx, samples := range recordData {
			// Store each sample in DB for corresponding signal
			signalID := int64(sigIdx + 1) // Assume signal IDs are sequential
			err := storeSamples(db, signalID, samples)
			if err != nil {
				return fmt.Errorf("error storing samples for signal %d: %w", sigIdx, err)
			}
		}
	}

	return nil
}

func StreamEDFToSQLitee(inputPath, dbPath string) error {
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening input file: %w", err)
	}
	defer inputFile.Close()

	db, err := initializeDB(dbPath)
	if err != nil {
		return fmt.Errorf("error initializing database: %w", err)
	}
	defer db.Close()

	header, signals, err := parseHeader(inputFile)
	if err != nil {
		return fmt.Errorf("header parsing failed: %w", err)
	}

	scalings := calculateScalingFactors(signals)

	// Process and store data in SQLite
	return ProcessRecords(inputFile, nil, header, signals, scalings, db)
}
