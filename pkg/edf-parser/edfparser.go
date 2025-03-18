package edfparser

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type Header struct {
	Version        string  `json:"version"`
	PatientID      string  `json:"patient_id"`
	RecordingID    string  `json:"recording_id"`
	StartDate      string  `json:"start_date"`
	StartTime      string  `json:"start_time"`
	HeaderBytes    int     `json:"header_bytes"`
	Reserved       string  `json:"reserved"`
	NRecords       int     `json:"num_records"`
	RecordDuration float64 `json:"record_duration"`
	NSignals       int     `json:"num_signals"`
}

type Signal struct {
	Label        string  `json:"label"`
	Transducer   string  `json:"transducer"`
	Units        string  `json:"units"`
	PhysicalMin  float64 `json:"physical_min"`
	PhysicalMax  float64 `json:"physical_max"`
	DigitalMin   int     `json:"digital_min"`
	DigitalMax   int     `json:"digital_max"`
	Prefiltering string  `json:"prefiltering"`
	NSamples     int     `json:"num_samples"`
	Reserved     string  `json:"reserved"`
}

type ScalingFactors struct {
	Scale  float64
	Offset float64
}

func StreamEDFToJSON(inputPath, outputPath string) error {
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening input file: %w", err)
	}
	defer inputFile.Close()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file: %w", err)
	}
	defer outputFile.Close()

	header, signals, err := parseHeader(inputFile)
	if err != nil {
		return fmt.Errorf("header parsing failed: %w", err)
	}

	scalings := calculateScalingFactors(signals)

	if err := writeJSONHeader(outputFile, header, signals); err != nil {
		return err
	}

	return processRecords(inputFile, outputFile, header, signals, scalings)
}

func parseHeader(file *os.File) (Header, []Signal, error) {
	headerBytes := make([]byte, 256)
	if _, err := io.ReadFull(file, headerBytes); err != nil {
		return Header{}, nil, fmt.Errorf("error reading main header: %w", err)
	}

	header := Header{
		Version:     strings.TrimSpace(string(headerBytes[0:8])),
		PatientID:   strings.TrimSpace(string(headerBytes[8:88])),
		RecordingID: strings.TrimSpace(string(headerBytes[88:168])),
		StartDate:   strings.TrimSpace(string(headerBytes[168:176])),
		StartTime:   strings.TrimSpace(string(headerBytes[176:184])),
		Reserved:    strings.TrimSpace(string(headerBytes[192:236])),
	}

	var err error
	header.HeaderBytes, err = parseHeaderInt(headerBytes[184:192])
	if err != nil {
		return Header{}, nil, fmt.Errorf("invalid header bytes: %w", err)
	}

	header.NRecords, err = parseHeaderInt(headerBytes[236:244])
	if err != nil {
		return Header{}, nil, fmt.Errorf("invalid record count: %w", err)
	}

	header.RecordDuration, err = parseHeaderFloat(headerBytes[244:252])
	if err != nil {
		return Header{}, nil, fmt.Errorf("invalid record duration: %w", err)
	}

	header.NSignals, err = parseHeaderInt(headerBytes[252:256])
	if err != nil {
		return Header{}, nil, fmt.Errorf("invalid signal count: %w", err)
	}

	signals, err := parseSignalHeaders(file, header.NSignals)
	if err != nil {
		return Header{}, nil, fmt.Errorf("error parsing signal headers: %w", err)
	}

	return header, signals, nil
}

func parseSignalHeaders(file *os.File, nSignals int) ([]Signal, error) {
	signalHeaderSize := nSignals * 256
	signalHeaderBytes := make([]byte, signalHeaderSize)
	if _, err := io.ReadFull(file, signalHeaderBytes); err != nil {
		return nil, fmt.Errorf("error reading signal headers: %w", err)
	}

	signals := make([]Signal, nSignals)
	parseField := func(offset, fieldSize int, setter func(int, string)) {
		for i := 0; i < nSignals; i++ {
			start := offset + i*fieldSize
			end := start + fieldSize
			if end > len(signalHeaderBytes) {
				end = len(signalHeaderBytes)
			}
			setter(i, strings.TrimSpace(string(signalHeaderBytes[start:end])))
		}
	}

	currentOffset := 0
	fields := []struct {
		size   int
		setter func(int, string)
	}{
		{16, func(i int, v string) { signals[i].Label = v }},
		{80, func(i int, v string) { signals[i].Transducer = v }},
		{8, func(i int, v string) { signals[i].Units = v }},
		{8, func(i int, v string) { signals[i].PhysicalMin, _ = strconv.ParseFloat(v, 64) }},
		{8, func(i int, v string) { signals[i].PhysicalMax, _ = strconv.ParseFloat(v, 64) }},
		{8, func(i int, v string) { signals[i].DigitalMin, _ = strconv.Atoi(v) }},
		{8, func(i int, v string) { signals[i].DigitalMax, _ = strconv.Atoi(v) }},
		{80, func(i int, v string) { signals[i].Prefiltering = v }},
		{8, func(i int, v string) { signals[i].NSamples, _ = strconv.Atoi(v) }},
		{32, func(i int, v string) { signals[i].Reserved = v }},
	}

	for _, field := range fields {
		parseField(currentOffset, field.size, field.setter)
		currentOffset += field.size * nSignals
	}

	return signals, nil
}

func calculateScalingFactors(signals []Signal) []ScalingFactors {
	scalings := make([]ScalingFactors, len(signals))
	for i, s := range signals {
		digitalRange := s.DigitalMax - s.DigitalMin
		if digitalRange == 0 {
			digitalRange = 1 // Prevent division by zero
		}
		scalings[i].Scale = (s.PhysicalMax - s.PhysicalMin) / float64(digitalRange)
		scalings[i].Offset = s.PhysicalMin - float64(s.DigitalMin)*scalings[i].Scale
	}
	return scalings
}

func writeJSONHeader(w io.Writer, header Header, signals []Signal) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	if _, err := w.Write([]byte("{\n  \"header\": ")); err != nil {
		return err
	}
	if err := enc.Encode(header); err != nil {
		return err
	}

	if _, err := w.Write([]byte(",\n  \"signals\": ")); err != nil {
		return err
	}
	if err := enc.Encode(signals); err != nil {
		return err
	}

	_, err := w.Write([]byte(",\n  \"data\": [\n"))
	return err
}

func processRecords(inputFile, outputFile *os.File, header Header, signals []Signal, scalings []ScalingFactors) error {
	bytesPerRecord := calculateBytesPerRecord(signals)
	recordBuffer := make([]byte, bytesPerRecord)
	enc := json.NewEncoder(outputFile)
	enc.SetIndent("    ", "  ")

	for rec := 0; rec < header.NRecords; rec++ {
		if _, err := io.ReadFull(inputFile, recordBuffer); err != nil {
			return fmt.Errorf("error reading record %d: %w", rec, err)
		}

		recordData, err := parseRecord(recordBuffer, signals, scalings)
		if err != nil {
			return fmt.Errorf("error parsing record %d: %w", rec, err)
		}

		if rec > 0 {
			if _, err := outputFile.Write([]byte(",\n")); err != nil {
				return err
			}
		}

		if err := enc.Encode(recordData); err != nil {
			return fmt.Errorf("error encoding record %d: %w", rec, err)
		}
	}

	_, err := outputFile.Write([]byte("\n  ]\n}"))
	return err
}

func calculateBytesPerRecord(signals []Signal) int {
	total := 0
	for _, s := range signals {
		total += s.NSamples * 2
	}
	return total
}

func parseRecord(buffer []byte, signals []Signal, scalings []ScalingFactors) ([][]float64, error) {
	data := make([][]float64, len(signals))
	offset := 0

	for sigIdx, signal := range signals {
		samples := make([]float64, signal.NSamples)
		requiredBytes := signal.NSamples * 2

		if offset+requiredBytes > len(buffer) {
			return nil, fmt.Errorf("buffer overflow in signal %d", sigIdx)
		}

		for i := 0; i < signal.NSamples; i++ {
			raw := int16(binary.LittleEndian.Uint16(buffer[offset : offset+2]))
			samples[i] = float64(raw)*scalings[sigIdx].Scale + scalings[sigIdx].Offset
			offset += 2
		}

		data[sigIdx] = samples
	}

	return data, nil
}

func parseHeaderInt(data []byte) (int, error) {
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

func parseHeaderFloat(data []byte) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
}
