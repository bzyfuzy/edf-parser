package main

import (
	"fmt"

	edfparser "github.com/bzyfuzy/edf-parser/pkg/edf-parser"
)

func main() {
	// edfparser.ECG_Process()
	err := edfparser.StreamEDFToSQLitee("/home/bzy/edfParser/20250110142035batkhaant.edf", "output_2_sql")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Successfully converted EDF to JSON")
}
