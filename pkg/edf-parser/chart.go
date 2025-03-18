package edfparser

import (
	"fmt"
)

type ChartData struct {
	Labels   []string  `json:"labels"`
	Datasets []Dataset `json:"datasets"`
}

type Dataset struct {
	Label       string    `json:"label"`
	Data        []float64 `json:"data"`
	BorderColor string    `json:"borderColor"`
	Fill        bool      `json:"fill"`
}

func ProcessEDFToChartData(header Header, signals []Signal, samples [][][]float64) (ChartData, error) {
	labels := make([]string, len(samples[0]))

	for i := 0; i < len(labels); i++ {

		labels[i] = fmt.Sprintf("Sample %d", i+1)
	}

	datasets := make([]Dataset, len(signals))

	for sigIdx, signal := range signals {
		dataset := Dataset{
			Label:       signal.Label,
			BorderColor: "rgb(75, 192, 192)",
			Fill:        false,
			Data:        make([]float64, len(samples[sigIdx])),
		}

		for sampleIdx := 0; sampleIdx < len(samples[sigIdx]); sampleIdx++ {
			dataset.Data[sampleIdx] = samples[sigIdx][sampleIdx][0]
		}

		datasets[sigIdx] = dataset
	}

	return ChartData{
		Labels:   labels,
		Datasets: datasets,
	}, nil
}
