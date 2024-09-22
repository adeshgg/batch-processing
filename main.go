package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// const url = "https://raw.githubusercontent.com/adeshgg/csv-data/refs/heads/main/sample.csv"

type RawData struct {
	Name   string
	Weight float64
	Height float64
	Batch  int
	BMI    float64 `json:"bmi"`
}

const NUM_BATCHES = 9

func main() {

	start := time.Now()

	const url = "https://raw.githubusercontent.com/adeshgg/csv-data/refs/heads/main/sample-data.csv"
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

	// Process BMI calculations concurrently
	err = processBMIConcurrently(rawDataArr)
	if err != nil {
		fmt.Printf("Error while processing BMI concurrently: %v", err)
		return
	}

	fmt.Println("Processed data:")
	for _, data := range rawDataArr {
		fmt.Printf("Name: %s, Weight: %.2f, Height: %.2f, Batch: %d, BMI: %.2f\n",
			data.Name, data.Weight, data.Height, data.Batch, data.BMI)
	}

	elapsed := time.Since(start)

	fmt.Printf("Total Execution time %v\n", elapsed)

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

func groupDataByBatch(rawDataArr []RawData) map[int][]*RawData {
	batchMap := make(map[int][]*RawData)
	for i := range rawDataArr {
		batch := rawDataArr[i].Batch
		batchMap[batch] = append(batchMap[batch], &rawDataArr[i])
	}
	return batchMap
}

func populateBmiValue(rawDataArr *RawData) {
	heightInMeters := rawDataArr.Height / 100

	rawDataArr.BMI = rawDataArr.Weight / (heightInMeters * heightInMeters)
}

func processBMIConcurrently(rawDataArr []RawData) error {
	var wg sync.WaitGroup

	errChan := make(chan error, NUM_BATCHES)

	batchMap := groupDataByBatch(rawDataArr)

	for batch := 1; batch <= NUM_BATCHES; batch++ {
		wg.Add(1)

		go func(batchNum int, batchData []*RawData) {
			defer wg.Done()
			for _, data := range batchData {
				populateBmiValue(data)
			}
		}(batch, batchMap[batch])
	}

	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}
