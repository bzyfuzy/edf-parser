package edfparser

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

// StreamEDFToSQLite converts an EDF file to a SQLite database, grouping data by signal.
func StreamEDFToSQLite(inputPath, outputPath string) error {
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening input file: %w", err)
	}
	defer inputFile.Close()

	db, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		return fmt.Errorf("error creating SQLite database: %w", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS header (
		version TEXT,
		patient_id TEXT,
		recording_id TEXT,
		start_date TEXT,
		start_time TEXT,
		header_bytes INTEGER,
		reserved TEXT,
		num_records INTEGER,
		record_duration REAL,
		num_signals INTEGER
	);

	CREATE TABLE IF NOT EXISTS signals (
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
		reserved TEXT
	);

	CREATE TABLE IF NOT EXISTS data (
		signal_id INTEGER,
		record_number INTEGER,
		samples BLOB,
		PRIMARY KEY (signal_id, record_number),
		FOREIGN KEY(signal_id) REFERENCES signals(id)
	);`)
	if err != nil {
		return fmt.Errorf("error creating tables: %w", err)
	}

	header, signals, err := parseHeader(inputFile)
	if err != nil {
		return fmt.Errorf("header parsing failed: %w", err)
	}

	// Insert header
	_, err = db.Exec(`INSERT INTO header (
		version, patient_id, recording_id, start_date, start_time,
		header_bytes, reserved, num_records, record_duration, num_signals
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		header.Version,
		header.PatientID,
		header.RecordingID,
		header.StartDate,
		header.StartTime,
		header.HeaderBytes,
		header.Reserved,
		header.NRecords,
		header.RecordDuration,
		header.NSignals,
	)
	if err != nil {
		return fmt.Errorf("error inserting header: %w", err)
	}

	// Insert signals
	for _, sig := range signals {
		_, err := db.Exec(`INSERT INTO signals (
			label, transducer, units, physical_min, physical_max,
			digital_min, digital_max, prefiltering, num_samples, reserved
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			sig.Label,
			sig.Transducer,
			sig.Units,
			sig.PhysicalMin,
			sig.PhysicalMax,
			sig.DigitalMin,
			sig.DigitalMax,
			sig.Prefiltering,
			sig.NSamples,
			sig.Reserved,
		)
		if err != nil {
			return fmt.Errorf("error inserting signal %s: %w", sig.Label, err)
		}
	}

	scalings := calculateScalingFactors(signals)

	return processRecordsSQLite(db, inputFile, header, signals, scalings)
}

func processRecordsSQLite(db *sql.DB, inputFile *os.File, header Header, signals []Signal, scalings []ScalingFactors) error {
	bytesPerRecord := calculateBytesPerRecord(signals)
	recordBuffer := make([]byte, bytesPerRecord)

	for rec := 0; rec < header.NRecords; rec++ {
		if _, err := io.ReadFull(inputFile, recordBuffer); err != nil {
			return fmt.Errorf("error reading record %d: %w", rec, err)
		}

		recordData, err := parseRecord(recordBuffer, signals, scalings)
		if err != nil {
			return fmt.Errorf("error parsing record %d: %w", rec, err)
		}

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("error starting transaction for record %d: %w", rec, err)
		}

		for sigIdx, samples := range recordData {
			signalID := sigIdx + 1 // Signal IDs start at 1
			samplesBytes := float64SliceToBytes(samples)

			_, err = tx.Exec(
				`INSERT INTO data (signal_id, record_number, samples) VALUES (?, ?, ?)`,
				signalID, rec, samplesBytes,
			)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("error inserting data for signal %d, record %d: %w", signalID, rec, err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("error committing transaction for record %d: %w", rec, err)
		}
	}

	return nil
}

func float64SliceToBytes(samples []float64) []byte {
	bytes := make([]byte, len(samples)*8)
	for i, f := range samples {
		binary.LittleEndian.PutUint64(bytes[i*8:(i+1)*8], math.Float64bits(f))
	}
	return bytes
}

// Existing functions (parseHeader, parseSignalHeaders, calculateScalingFactors, calculateBytesPerRecord, parseRecord, parseHeaderInt, parseHeaderFloat) remain unchanged from the original code.
