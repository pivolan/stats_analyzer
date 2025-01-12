package main

import (
	"encoding/csv"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestGenerateCommonInfoMsg(t *testing.T) {
	// Create test data
	stats := map[string]CommonStat{
		"all": {
			Count: 1000,
		},
		"age": {
			IsNumeric:  true,
			Avg:        35.5,
			Median:     34.0,
			Quantile01: 18.0,
			Quantile09: 65.0,
			Groups: []map[string]interface{}{
				{
					"value": 25,
					"count": 150,
				},
			},
		},
		"salary": {
			IsNumeric:  true,
			Avg:        75000.0,
			Median:     70000.0,
			Quantile01: 45000.0,
			Quantile09: 120000.0,
		},
		"name": {
			IsNumeric: false,
			Uniq:      450,
			Groups: []map[string]interface{}{
				{
					"value": "John",
					"count": 30,
				},
			},
		},
		"created_at": {
			IsNumeric: false,
			Uniq:      800,
			Dates:     []map[string]interface{}{{}}, // Just need non-empty to indicate date
		},
	}

	// Generate message
	result := GenerateCommonInfoMsg(stats)

	// Print the result
	fmt.Println("Generated Message:")
	fmt.Println("-------------------")
	fmt.Println(result)
	fmt.Println("-------------------")

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

func TestGenerateTable(t *testing.T) {
	stats := map[string]CommonStat{
		"user_age": {
			Count:       1000,
			Uniq:        120,
			Avg:         27.5,
			Min:         18.0,
			Max:         65.0,
			Median:      28.0,
			Quantile001: 18.5,
			Quantile01:  19.0,
			Quantile099: 62.5,
			Quantile09:  55.0,
		},
		"order_amount": {
			Count:       5000,
			Uniq:        450,
			Avg:         149.99,
			Min:         10.0,
			Max:         999.99,
			Median:      125.0,
			Quantile001: 15.0,
			Quantile01:  25.0,
			Quantile099: 899.99,
			Quantile09:  599.99,
		},
		"response_time_ms": {
			Count:       10000,
			Uniq:        789,
			Avg:         245.5,
			Min:         50.0,
			Max:         1500.0,
			Median:      200.0,
			Quantile001: 55.0,
			Quantile01:  75.0,
			Quantile099: 1250.0,
			Quantile09:  750.0,
		},
	}

	result := GenerateTable(stats)

	// Check that the table contains our field names
	expectedFields := []string{"user_age", "order_amount", "response_time_ms"}
	for _, field := range expectedFields {
		if !strings.Contains(result, field) {
			t.Errorf("Generated table doesn't contain field name: %s", field)
		}
	}

	// Check specific statistical values
	expectedValues := []string{
		"1000", "120", "27.5", // user_age values
		"5000", "450", "149.99", // order_amount values
		"10000", "789", "245.5", // response_time_ms values
	}
	for _, value := range expectedValues {
		if !strings.Contains(result, value) {
			t.Errorf("Generated table doesn't contain expected value: %s", value)
		}
	}
	assert.Equal(t,
		`+------------------+-------+------+--------+-----+--------+--------+-------------+------------+-------------+------------+-------+--------+
| FIELDNAME        | COUNT | UNIQ | AVG    | MIN | MAX    | MEDIAN | QUANTILE001 | QUANTILE01 | QUANTILE099 | QUANTILE09 | DATES | GROUPS |
+------------------+-------+------+--------+-----+--------+--------+-------------+------------+-------------+------------+-------+--------+
| order_amount     |  5000 |  450 | 149.99 | 10  | 999.99 | 125    | 15          | 25         | 899.99      | 599.99     |       |        |
| response_time_ms | 10000 |  789 | 245.5  | 50  | 1500   | 200    | 55          | 75         | 1250        | 750        |       |        |
| user_age         |  1000 |  120 | 27.5   | 18  | 65     | 28     | 18.5        | 19         | 62.5        | 55         |       |        |
+------------------+-------+------+--------+-----+--------+--------+-------------+------------+-------------+------------+-------+--------+`, result)
}
func TestGenerateCSVByDates(t *testing.T) {
	stats := map[string]CommonStat{
		"test": {
			Dates: []map[string]interface{}{
				{"date": "2024-01-01", "value": 42},
				{"date": "2024-01-02", "value": 84},
			},
		},
	}

	results := GenerateCSVByDates(stats)

	if len(results) != 1 {
		t.Errorf("Expected 1 CSV, got %d", len(results))
	}

	// Parse the CSV content
	reader := csv.NewReader(strings.NewReader(results["test"]))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	// Check header
	if len(records) < 1 {
		t.Fatal("CSV has no records")
	}
	header := records[0]
	if !contains(header, "date") || !contains(header, "value") {
		t.Error("CSV header doesn't contain expected columns")
	}
}

// Helper function to check if a slice contains a string
func contains(slice []string, str string) bool {
	for _, v := range slice {
		if v == str {
			return true
		}
	}
	return false
}
