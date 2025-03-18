package edfparser

import (
	"fmt"
)

// ChartData represents the structure for Chart.js
type ChartData struct {
	Labels   []string  `json:"labels"`   // X-axis data, typically time or sample index
	Datasets []Dataset `json:"datasets"` // Array of signal datasets
}

// Dataset represents a single signal in the Chart.js dataset
type Dataset struct {
	Label       string    `json:"label"`
	Data        []float64 `json:"data"`        // Y-axis values
	BorderColor string    `json:"borderColor"` // Color for the signal line
	Fill        bool      `json:"fill"`        // Whether to fill the area under the curve
}

// ProcessEDFToChartData takes the parsed EDF data and returns it in Chart.js format
func ProcessEDFToChartData(header Header, signals []Signal, samples [][][]float64) (ChartData, error) {
	// Initialize the labels array (usually representing timestamps or sample indices)
	labels := make([]string, len(samples[0])) // Assuming all signals have the same sample size

	// Create the timestamps or sample indices for the x-axis (labels)
	for i := 0; i < len(labels); i++ {
		// If you want timestamps, modify this to the actual time logic
		labels[i] = fmt.Sprintf("Sample %d", i+1)
	}

	// Prepare the datasets for Chart.js
	datasets := make([]Dataset, len(signals))

	for sigIdx, signal := range signals {
		// Map the signal data to the dataset
		dataset := Dataset{
			Label:       signal.Label,
			BorderColor: "rgb(75, 192, 192)", // Default color, you can change as needed
			Fill:        false,               // Do not fill the area under the curve
			Data:        make([]float64, len(samples[sigIdx])),
		}

		// Extract the signal data
		for sampleIdx := 0; sampleIdx < len(samples[sigIdx]); sampleIdx++ {
			// Process the raw samples using scaling factors and store them in the dataset
			dataset.Data[sampleIdx] = samples[sigIdx][sampleIdx][0] // Assuming a single channel of data per signal
		}

		// Append the dataset to the datasets array
		datasets[sigIdx] = dataset
	}

	// Return the final chart data
	return ChartData{
		Labels:   labels,
		Datasets: datasets,
	}, nil
}
