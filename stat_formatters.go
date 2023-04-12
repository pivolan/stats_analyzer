package main

import (
	"fmt"
	"reflect"
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
		// Add a new row to the table with the key and values from the CommonStat struct
		t.AppendRows([]table.Row{
			{k, v.Uniq, v.Avg, v.Min, v.Max, v.Median, v.Quantile001, v.Quantile099},
		})
	}

	// Set the output format of the table to Markdown
	t.SetStyle(table.StyleDefault)

	// Render the table and return it as a string
	return t.Render()
}

func GenerateTableAscii(stats map[string]CommonStat) string {
	// Initialize buffer with headers
	buf := &strings.Builder{}
	buf.WriteString(fmt.Sprintf("%-15s", "Key"))

	// Get the type of the CommonStat struct using reflection
	cType := reflect.TypeOf(CommonStat{})

	// Loop over each field in the CommonStat struct and add a header column to the table
	for i := 0; i < cType.NumField(); i++ {
		field := cType.Field(i)
		buf.WriteString(fmt.Sprintf(" %-15s", field.Name))
	}

	buf.WriteString("\n")

	// Loop over each key in the map and add a row to the table
	for k, v := range stats {
		// Write the key to the buffer
		buf.WriteString(fmt.Sprintf("%-15s", k))

		// Loop over each field in the CommonStat struct and add a data column to the table
		for i := 0; i < cType.NumField(); i++ {
			field := cType.Field(i)
			value := reflect.ValueOf(v).FieldByName(field.Name).Interface()
			switch value.(type) {
			case int64:
				buf.WriteString(fmt.Sprintf(" %-15d", value.(int64)))
			case float64:
				buf.WriteString(fmt.Sprintf(" %-15s", strconv.FormatFloat(value.(float64), 'f', 3, 64)))
			default:
				buf.WriteString(fmt.Sprintf(" %-15v", value))
			}
		}

		buf.WriteString("\n")
	}

	// Return the table as a string
	return buf.String()
}

func GenerateTableMarkdown(stats map[string]CommonStat) string {
	// Initialize buffer with headers
	buf := &strings.Builder{}
	buf.WriteString("| Key |")

	// Get the type of the CommonStat struct using reflection
	cType := reflect.TypeOf(CommonStat{})

	// Loop over each field in the CommonStat struct and add a header column to the table
	for i := 0; i < cType.NumField(); i++ {
		field := cType.Field(i)
		buf.WriteString(fmt.Sprintf(" %s |", field.Name))
	}

	buf.WriteString("\n")

	// Loop over each key in the map and add a row to the table
	for k, v := range stats {
		// Write the key to the buffer
		buf.WriteString(fmt.Sprintf("| %s |", k))

		// Loop over each field in the CommonStat struct and add a data column to the table
		for i := 0; i < cType.NumField(); i++ {
			field := cType.Field(i)
			value := reflect.ValueOf(v).FieldByName(field.Name).Interface()
			switch value.(type) {
			case int64:
				buf.WriteString(fmt.Sprintf(" %d |", value.(int64)))
			case float64:
				buf.WriteString(fmt.Sprintf(" %.3f |", value.(float64)))
			default:
				buf.WriteString(fmt.Sprintf(" %v |", value))
			}
		}

		buf.WriteString("\n")
	}

	// Return the table as a string
	return buf.String()
}
