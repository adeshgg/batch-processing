package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// const url = "https://raw.githubusercontent.com/adeshgg/csv-data/refs/heads/main/sample.csv"

type RawData struct {
	Name   string
	Weight float64
	Height float64
	Batch  int
	BMI    float64 `json:"bmi"`
}

const NUM_BATCHES = 2

func main() {

	const url = "https://raw.githubusercontent.com/adeshgg/csv-data/refs/heads/main/sample.csv"
	rawDataArr, err := fetchAndPopulateRawData(url)

	if err != nil {
		fmt.Printf("Error while parsing the csv file: %v", err)
		return
	}

	rawDataArr, err = assignBatches(rawDataArr)

	if err != nil {
		fmt.Printf("Error while assigning batches: %v", err)
		return
	}

}

func fetchAndPopulateRawData(url string) ([]RawData, error) {

	// Fetch the CSV file from URL
	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error fetching CSV file: %v\n", err)
		return []RawData{}, err
	}

	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		fmt.Printf("Bad status code: %d\n", response.StatusCode)
		return []RawData{}, err
	}

	reader := csv.NewReader(response.Body)

	// Read the header row
	if header, err := reader.Read(); err == nil {
		fmt.Printf("Header: %v\n", header) // Optional: Print header for verification
	} else {
		fmt.Printf("Error Reading CSV header: %v\n", err)
		return []RawData{}, err
	}

	var rawDataArr []RawData

	// Read and print each row
	for {
		record, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Printf("Error Reading CSV: %v\n", err)
			return []RawData{}, err
		}

		var rawData RawData
		for index, data := range record {

			legend := ""

			if index == 0 {
				legend = "Name"
				rawData.Name = data
			} else {
				floatData, _ := strconv.ParseFloat(data, 64)

				if index == 1 {
					rawData.Height = floatData
				} else {
					rawData.Weight = floatData
				}
			}
			fmt.Printf("%s : %s ", legend, data)

		}
		fmt.Println()
		rawDataArr = append(rawDataArr, rawData)
	}

	return rawDataArr, nil
}

func assignBatches(rawDataArr []RawData) ([]RawData, error) {

	for index := range rawDataArr {
		rawDataArr[index].Batch = (index%NUM_BATCHES + 1)
	}

	return rawDataArr, nil
}

func populateBmiValue(rawDataArr RawData) {
	heightInMeters := rawDataArr.Height / 100

	rawDataArr.BMI = rawDataArr.Weight / (heightInMeters * heightInMeters)
}
