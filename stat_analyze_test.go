package main

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAnalyzeStatistics(t *testing.T) {
	result := analyzeStatistics("default.id_time_project_576de3")
	//generate by date fields
	fmt.Println(result)
	//groups
}
func TestCSV(t *testing.T) {
	results := handleFile("/Users/igorpecenikin/Downloads/roularta_dataset/koef.csv")
	results = handleFile("/Users/igorpecenikin/PhpstormProjects/bottalk/bfp-backend6/docs/user.csv")
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
