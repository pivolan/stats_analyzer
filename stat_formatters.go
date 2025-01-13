package main

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

import (
	"github.com/jedib0t/go-pretty/v6/table"
)

func GenerateCommonInfoMsg(stats map[string]CommonStat) string {
	var result strings.Builder

	// Total Records
	if allStat, ok := stats["all"]; ok {
		result.WriteString(fmt.Sprintf("📊 Total Lines: %d\n\n", allStat.Count))
	}

	// Collect numeric and string columns
	var numericColumns, stringColumns, dateColumns []string
	for field, stat := range stats {
		if field == "all" {
			continue
		}
		if stat.IsNumeric {
			numericColumns = append(numericColumns, field)
		} else if stat.Uniq > 0 || len(stat.Groups) > 0 {
			stringColumns = append(stringColumns, field)
		} else if len(stat.Dates) > 0 {
			dateColumns = append(dateColumns, field)
		}
	}

	// Sort columns alphabetically
	sort.Strings(numericColumns)
	sort.Strings(stringColumns)
	sort.Strings(dateColumns)

	// Numeric Columns
	if len(numericColumns) > 0 {
		result.WriteString("📈 Numeric Columns:\n")
		for _, field := range numericColumns {
			stat := stats[field]
			result.WriteString(fmt.Sprintf("• %s\n", field[5:]))
			result.WriteString(fmt.Sprintf("  Avg: %.2f\n", stat.Avg))
			result.WriteString(fmt.Sprintf("  Median: %.2f\n", stat.Median))
			result.WriteString(fmt.Sprintf("  90%% of values between: %.2f - %.2f\n", stat.Quantile01, stat.Quantile09))

			// Add graph shortcut
			result.WriteString(fmt.Sprintf("  /graph_%s\n\n", field))
		}
	}

	// String Columns
	if len(stringColumns) > 0 {
		result.WriteString("📝 Text Columns:\n")
		for _, field := range stringColumns {
			stat := stats[field]
			if stat.Uniq > 0 {
				result.WriteString(fmt.Sprintf("• %s (%d unique values); /details_%s\n", field[5:], stat.Uniq, field))
			}

			// Check if we have group data
			if len(stat.Groups) > 0 {
				// Find the most frequent value
				maxCount := int64(0)
				var maxValue string
				columnName := ""
				for _, group := range stat.Groups {
					for columnName, _ = range group {
						if columnName != "count()" {
							break
						}
					}
					if count, ok := group["count()"].(int64); ok {
						if count > maxCount {
							maxCount = count
							maxValue = fmt.Sprintf("%v", group[columnName])
						}
					}
				}
				if maxValue != "" {
					result.WriteString(fmt.Sprintf("  Most frequent: %s (%d times)\n", maxValue, maxCount))
				}
			}

			// Check if it's a date column
			if len(stat.Dates) > 0 {
				result.WriteString("  (detected as date)\n")
			}
			result.WriteString("\n")
		}
	}
	if len(dateColumns) > 0 {
		result.WriteString("📝 Date Columns:\n")
		for _, field := range dateColumns {
			//stat := stats[field]
			result.WriteString(fmt.Sprintf("• %s; /details_%s\n", field[11:], field))
		}
	}
	return result.String()
}

func GenerateTable(stats map[string]CommonStat) string {
	// Create a new table with a Key column and columns for each field in the CommonStat struct
	t := table.NewWriter()
	t1 := reflect.TypeOf(CommonStat{})

	// Loop over the fields in the struct and print their names
	fields := table.Row{"FieldName"}
	for i := 0; i < t1.NumField(); i++ {
		field := t1.Field(i)
		fields = append(fields, field.Name)
	}
	t.AppendHeader(fields)

	var keys []string
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Now use sorted keys to process values
	for _, k := range keys {
		v := stats[k]
		values := make([]interface{}, len(fields))
		values[0] = k

		// Loop over the fields in the CommonStat struct and add their values to the row
		for i, h := range fields[1:] {
			if f, ok := reflect.TypeOf(v).FieldByName(h.(string)); ok {
				if f.Type.Kind() == reflect.Int64 {
					values[i+1] = reflect.ValueOf(v).FieldByName(h.(string)).Int()
				} else if f.Type.Kind() == reflect.Float64 {
					val := reflect.ValueOf(v).FieldByName(h.(string)).Float()
					a := strconv.FormatFloat(val, 'f', 3, 64)
					a = strings.TrimRight(a, "0")
					a = strings.TrimRight(a, ".")

					values[i+1] = a
				} else {
					values[i+1] = ""
				}
			}
		}

		// Add a new row to the table with the key and values from the CommonStat struct
		t.AppendRows([]table.Row{values})
	}

	// Set the output format of the table to Markdown
	t.SetStyle(table.StyleDefault)

	// Render the table and return it as a string
	return t.Render()
}
func GenerateGroupsTables(stats map[string]CommonStat) []string {
	// Create a new table with a Key column and columns for each field in the CommonStat struct
	result := []string{}
	// Loop over each key in the map and add a row to the table
	headers := []string{}
	for header, _ := range stats {
		headers = append(headers, header)
	}
	sort.Strings(headers)
	for _, h := range headers {
		v := stats[h]
		t := table.NewWriter()

		// Loop over the fields in the struct and print their names
		fields := table.Row{}
		if len(v.Groups) == 0 {
			continue
		}
		mapRevert := map[string]int{}
		i := 0
		header := []string{}

		for columnName, _ := range v.Groups[0] {
			header = append(header, columnName)
		}
		sort.Strings(header)
		for _, columnName := range header {
			fields = append(fields, columnName)
			mapRevert[columnName] = i
			i++
		}

		t.AppendHeader(fields)

		for _, data := range v.Groups {
			values := make([]interface{}, len(data))
			for column, value := range data {
				values[mapRevert[column]] = value
			}
			t.AppendRows([]table.Row{values})
		}

		// Add a new row to the table with the key and values from the CommonStat struct
		t.SetStyle(table.StyleDefault)

		result = append(result, v.Title)
		result = append(result, t.Render())
	}

	// Set the output format of the table to Markdown
	// Render the table and return it as a string
	return result
}
func GenerateCSVByDates(stats map[string]CommonStat) map[string]string {
	result := map[string]string{}

	// Loop over each key in the map and add a row to the table
	for name, v := range stats {
		var buffer bytes.Buffer
		t := csv.NewWriter(&buffer)

		// Loop over the fields in the struct and print their names
		fields := []string{}
		if len(v.Dates) == 0 {
			continue
		}
		mapRevert := map[string]int{}
		i := 0
		header := []string{}

		for columnName, _ := range v.Dates[0] {
			header = append(header, columnName)
		}
		sort.Strings(header)
		for _, columnName := range header {
			fields = append(fields, columnName)
			mapRevert[columnName] = i
			i++
		}

		t.Write(fields)

		for _, data := range v.Dates {
			values := make([]string, len(data))
			for column, value := range data {
				values[mapRevert[column]] = fmt.Sprint(value)
			}
			t.Write(values)
		}

		// Add a new row to the table with the key and values from the CommonStat struct
		t.Flush()
		result[name] = buffer.String()
	}

	// Set the output format of the table to Markdown
	// Render the table and return it as a string
	return result
}
func ZipArchive(csvs map[string]string) []byte {
	var buffer bytes.Buffer

	// Step 2: Create a new ZIP writer using the buffer
	zipWriter := zip.NewWriter(&buffer)

	// Step 3: For each string/file pair
	for i, content := range csvs {
		// Create a new entry using the header
		writer, err := zipWriter.Create(fmt.Sprintf("%s.csv", i))
		if err != nil {
			return nil
		}

		// Write the string content to this entry
		_, err = writer.Write([]byte(content))
		if err != nil {
			return nil
		}
	}

	// Step 4: Close the ZIP writer
	zipWriter.Close()

	// Print the ZIP content for demonstration (usually you'd save this to a file or send it somewhere)
	return buffer.Bytes()
}
