package main

import (
	"fmt"
	"testing"
)

func TestAnalyzeStatistics(t *testing.T) {
	result := analyzeStatistics("default.id_time_project_576de3")
	//generate by date fields
	fmt.Println(result)
	//groups
}
