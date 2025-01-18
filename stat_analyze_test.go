package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAnalyzeStatistics(t *testing.T) {
	result := analyzeStatistics("default.id_time_project_576de3")
	//generate by date fields
	fmt.Println(result)
	//groups
}
func TestCSV(t *testing.T) {
	results, _ := handleFile("/Users/igorpecenikin/Downloads/a.csv")
	data, _ := json.MarshalIndent(results, "", "\t")
	fmt.Printf("%+v\n", string(data))
}
func TestPriorityTypeChooser(t *testing.T) {
	typesPriority := []string{"String", "Float64", "Int64", "Date", ""}
	types := []string{""}
	_t := "Float64"
	fmt.Println(SearchStrings(typesPriority, _t))
	fmt.Println(SearchStrings(typesPriority, types[0]))
	assert.True(t, SearchStrings(typesPriority, _t) < SearchStrings(typesPriority, types[0]))
}
func TestTimeParser(t *testing.T) {
	const layout = "2006-01-02 15:04:05.999999"
	str := "2022-10-26 06:03:18.272132"

	tt, err := time.Parse(layout, str)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	assert.Equal(t, "2022-10-26 06:03:18.272132 +0000 UTC", fmt.Sprint(tt))
}
