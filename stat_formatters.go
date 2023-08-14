package main

import (
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

func GenerateTable(stats map[string]CommonStat) string {
	// Create a new table with a Key column and columns for each field in the CommonStat struct
	t := table.NewWriter()
	t1 := reflect.TypeOf(CommonStat{})

	// Loop over the fields in the struct and print their names
	fields := table.Row{"FieldName"}
	for i := 0; i < t1.NumField(); i++ {
		field := t1.Field(i)
		fmt.Println(field.Name)
		fields = append(fields, field.Name)
	}
	t.AppendHeader(fields)

	// Loop over each key in the map and add a row to the table
	for k, v := range stats {
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
	for _, v := range stats {
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

		result = append(result, t.Render())
	}

	// Set the output format of the table to Markdown
	// Render the table and return it as a string
	return result
}
func GenerateCSVByDates(stats map[string]CommonStat) []string {
	result := []string{}

	// Loop over each key in the map and add a row to the table
	for _, v := range stats {
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
		result = append(result, buffer.String())
	}

	// Set the output format of the table to Markdown
	// Render the table and return it as a string
	return result
}
