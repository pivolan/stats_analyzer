package main

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestAnalyzeStatistics(t *testing.T) {
	result := analyzeStatistics("default.id_time_project_576de3")
	//generate by date fields
	fmt.Println(result)
	//groups
}
func TestCSV(t *testing.T) {
	results := handleFile("/Users/igorpecenikin/Downloads/a.csv")
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
func TestGenerateGroupsTables(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	info := map[string]CommonStat{
		"vasya": {Groups: []map[string]interface{}{
			{"id": 10, "datetime": baseTime, "count(*)": 123},
			{"id": 11, "datetime": baseTime.Add(time.Minute), "count(*)": 12},
			{"id": 12, "datetime": baseTime.Add(time.Minute * 2), "count(*)": 22},
		}},
		"kolya": {Groups: []map[string]interface{}{
			{"id": 10, "datetime": baseTime, "count(*)": 123},
			{"id": 11, "datetime": baseTime.Add(time.Minute), "count(*)": 12},
			{"id": 12, "datetime": baseTime.Add(time.Minute * 2), "count(*)": 22},
		}},
	}
	result := GenerateGroupsTables(info)
	assert.Equal(t, `+----------+-------------------------------+----+
| COUNT(*) | DATETIME                      | ID |
+----------+-------------------------------+----+
|      123 | 2024-01-01 10:00:00 +0000 UTC | 10 |
|       12 | 2024-01-01 10:01:00 +0000 UTC | 11 |
|       22 | 2024-01-01 10:02:00 +0000 UTC | 12 |
+----------+-------------------------------+----+
+----------+-------------------------------+----+
| COUNT(*) | DATETIME                      | ID |
+----------+-------------------------------+----+
|      123 | 2024-01-01 10:00:00 +0000 UTC | 10 |
|       12 | 2024-01-01 10:01:00 +0000 UTC | 11 |
|       22 | 2024-01-01 10:02:00 +0000 UTC | 12 |
+----------+-------------------------------+----+`, strings.Join(result, "\n"))
}
