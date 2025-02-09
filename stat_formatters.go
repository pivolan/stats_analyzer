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

	"github.com/jedib0t/go-pretty/v6/table"
)

func GenerateCommonInfoMsg(stats map[string]CommonStat) string {
	var result strings.Builder

	// Total Records
	if allStat, ok := stats["all"]; ok {
		result.WriteString(fmt.Sprintf("📊 Total Lines: %d\n\n", allStat.Count))
	}

	// Numeric Columns
	result.WriteString("📈 Numeric Columns:\n")
	for name, stat := range stats {
		if stat.IsNumeric {
			columnName := strings.TrimPrefix(name, "0003_")
			result.WriteString(fmt.Sprintf("• %s\n", columnName))
			result.WriteString(fmt.Sprintf("  Avg: %.2f\n", stat.Avg))
			result.WriteString(fmt.Sprintf("  Median: %.2f\n", stat.Median))
			result.WriteString(fmt.Sprintf("  90%% of values between: %.2f - %.2f\n", stat.Quantile01, stat.Quantile09))
			result.WriteString(fmt.Sprintf("  /graph_%s\n", name))
		}
	}
	result.WriteString("\n")

	// Text Columns
	result.WriteString("\n📝 Text Columns:")
	processedColumns := make(map[string]bool)
	// isFirstColumn := true

	for name, stat := range stats {
		if !stat.IsNumeric && !strings.HasPrefix(name, "dates_") {
			baseName := strings.TrimPrefix(name, "0002_")
			if processedColumns[baseName] {
				continue
			}

			if stat.Uniq > 0 {
				result.WriteString(fmt.Sprintf("\n• %s (%d unique values); /details_%s\n",
					baseName, stat.Uniq, name))
			}

			// Самое частое значение
			maxCount := int64(0)
			var mostFrequent string
			if len(stat.Groups) > 0 && strings.Contains(stat.Title, "частые") {
				for _, group := range stat.Groups {
					count := int64(0)
					value := ""
					for k, v := range group {
						if k != "count" && k != "percentage" {
							value = fmt.Sprintf("%v", v)
						}
						if k == "count" {
							if c, ok := v.(int64); ok {
								count = c
							}
						}
					}
					if count > maxCount {
						maxCount = count
						mostFrequent = value
					}
				}
				if mostFrequent != "" {
					result.WriteString(fmt.Sprintf("Most frequent: %s (%d times)\n",
						mostFrequent, maxCount))
				}
			}

			// Самое редкое значение
			rareName := name + "_rare"
			minCount := int64(^uint64(0) >> 1)
			var rarest string
			if rareStat, exists := stats[rareName]; exists && len(rareStat.Groups) > 0 {
				for _, group := range rareStat.Groups {
					count := int64(0)
					value := ""
					for k, v := range group {
						if k != "count" && k != "percentage" {
							value = fmt.Sprintf("%v", v)
						}
						if k == "count" {
							if c, ok := v.(int64); ok {
								count = c
							}
						}
					}
					if count < minCount {
						minCount = count
						rarest = value
					}
				}
				if rarest != "" {
					result.WriteString(fmt.Sprintf("Most rare: %s (%d times)",
						rarest, minCount))
				}
			}
			processedColumns[baseName] = true

		}
	}
	result.WriteString("\n\n")
	// Date Columns
	if hasDates := false; true {
		for name, stat := range stats {
			if strings.HasPrefix(name, "dates_") && len(stat.Dates) > 0 {
				if !hasDates {
					result.WriteString("📅 Date Columns:\n")
					hasDates = true
				}
				fieldName := strings.TrimPrefix(name, "dates_0001_")
				if strings.Contains(name, "day") {
					result.WriteString(fmt.Sprintf("• %s (day); /%s\n",
						strings.TrimSuffix(fieldName, "_day"), name))
				} else if strings.Contains(name, "month") {
					result.WriteString(fmt.Sprintf("• %s (month); /%s\n",
						strings.TrimSuffix(fieldName, "_month"), name))
				} else if strings.Contains(name, "year") {
					result.WriteString(fmt.Sprintf("• %s (year); /%s\n",
						strings.TrimSuffix(fieldName, "_year"), name))
				} else if strings.Contains(name, "hour") {
					result.WriteString(fmt.Sprintf("• %s (hour); /%s\n",
						strings.TrimSuffix(fieldName, "_hour"), name))

				}
			}
		}
	}

	return result.String()
}

func isNumericColumn(name string) bool {
	// Проверяем, начинается ли имя с цифр (0001_, 0002_ и т.д.)
	if len(name) >= 4 && name[0:4] == "0003" {
		return true
	}
	return false
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
