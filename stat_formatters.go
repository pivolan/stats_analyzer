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

type SortableRow struct {
	FieldName string
	Values    []interface{}
	Original  table.Row
}

func GenerateTable(stats map[string]CommonStat) string {
	t := table.NewWriter()
	t1 := reflect.TypeOf(CommonStat{})
	commonStatCH := <-ChanelOriginOrder

	// Определяем, какие поля содержат данные и сохраняем их в порядке определения в структуре
	usedFields := make([]string, 0)

	for i := 0; i < t1.NumField(); i++ {
		field := t1.Field(i)

		for _, v := range stats {
			val := reflect.ValueOf(v)
			value := val.FieldByName(field.Name)

			if field.Type.Kind() == reflect.Int64 && value.Int() != 0 {
				usedFields = append(usedFields, field.Name)
				break
			} else if field.Type.Kind() == reflect.Float64 && value.Float() != 0 {
				usedFields = append(usedFields, field.Name)
				break
			}

		}
	}

	// Создаем заголовок с сохраненным порядком полей
	fields := table.Row{"FieldName"}
	for _, fieldName := range usedFields {
		fields = append(fields, fieldName)
	}
	t.AppendHeader(fields)

	// Собираем все строки для сортировки
	var sortableRows []SortableRow

	for k, v := range stats {
		values := make([]interface{}, 0)
		values = append(values, k)

		hasNonZeroValues := false
		for _, fieldName := range usedFields {
			if f, ok := reflect.TypeOf(v).FieldByName(fieldName); ok {
				if f.Type.Kind() == reflect.Int64 {
					val := reflect.ValueOf(v).FieldByName(fieldName).Int()
					values = append(values, val)
					if val != 0 {
						hasNonZeroValues = true
					}
				} else if f.Type.Kind() == reflect.Float64 {
					val := reflect.ValueOf(v).FieldByName(fieldName).Float()
					if val != 0 {
						hasNonZeroValues = true
					}
					a := strconv.FormatFloat(val, 'f', 3, 64)
					a = strings.TrimRight(a, "0")
					a = strings.TrimRight(a, ".")
					values = append(values, a)
				}
			}
		}

		if hasNonZeroValues || k == "all" {
			row := table.Row(values)
			sortableRow := SortableRow{
				FieldName: k,
				Values:    values,
				Original:  row,
			}
			sortableRows = append(sortableRows, sortableRow)
		}
	}

	for i := range commonStatCH {
		for v := range sortableRows {
			if commonStatCH[i] == sortableRows[v].FieldName {
				sortableRows[v], sortableRows[i] = sortableRows[i], sortableRows[v]
				// sortableRow1 = append(sortableRow1, sortableRows[v])
				break
			}
		}
		// Сортируем только для перемещения "all" в конец
		sort.Slice(sortableRows, func(i, j int) bool {
			// Special handling for "all" row - it should always be last

			if sortableRows[i].FieldName == "all" {
				return false
			}
			if sortableRows[j].FieldName == "all" {
				return true
			}
			// Сохраняем исходный порядок для остальных строк

			return i < j
		})
		// commonStatCH = append(commonStatCH, "all")

	}

	// Добавляем отсортированные строки
	for _, row := range sortableRows {
		t.AppendRow(row.Original)
	}

	t.SetStyle(table.StyleDefault)
	return t.Render()
}

func GenerateGroupsTables(stats map[string]CommonStat) []string {
	result := []string{}

	for _, v := range stats {
		if len(v.Groups) == 0 {
			continue
		}

		t := table.NewWriter()

		// Находим колонки, содержащие ненулевые значения
		usedColumns := make(map[string]bool)
		for _, row := range v.Groups {
			for columnName, value := range row {
				// Проверяем числовые значения и строки
				switch val := value.(type) {
				case float64:
					if val != 0 {
						usedColumns[columnName] = true
					}
				case int64:
					if val != 0 {
						usedColumns[columnName] = true
					}
				case string:
					if val != "" {
						usedColumns[columnName] = true
					}
				}
			}
		}

		// Получаем и сортируем имена используемых колонок
		header := make([]string, 0)
		for columnName := range usedColumns {
			header = append(header, columnName)
		}
		sort.Strings(header)

		// Если нет используемых колонок, пропускаем таблицу
		if len(header) == 0 {
			continue
		}

		// Создаем отображение для индексов колонок
		columnIndices := make(map[string]int)
		for i, columnName := range header {
			columnIndices[columnName] = i
		}

		// Добавляем отсортированный заголовок
		fields := table.Row{}
		for _, columnName := range header {
			fields = append(fields, columnName)
		}
		t.AppendHeader(fields)

		// Добавляем данные в отсортированном порядке
		for _, data := range v.Groups {
			values := make([]interface{}, len(header))
			hasData := false
			for column, value := range data {
				if usedColumns[column] {
					values[columnIndices[column]] = value
					hasData = true
				}
			}
			if hasData {
				t.AppendRows([]table.Row{values})
			}
		}

		t.SetStyle(table.StyleDefault)
		result = append(result, t.Render())
	}

	return result
}

func GenerateCSVByDates(stats map[string]CommonStat) map[string]string {
	result := make(map[string]string)

	for name, v := range stats {
		if len(v.Dates) == 0 {
			continue
		}

		var buffer bytes.Buffer
		writer := csv.NewWriter(&buffer)

		// Находим колонки, содержащие ненулевые значения
		usedColumns := make(map[string]bool)
		for _, row := range v.Dates {
			for columnName, value := range row {
				// Проверяем числовые значения и строки
				switch val := value.(type) {
				case float64:
					if val != 0 {
						usedColumns[columnName] = true
					}
				case int64:
					if val != 0 {
						usedColumns[columnName] = true
					}
				case string:
					if val != "" {
						usedColumns[columnName] = true
					}
				}
			}
		}

		// Получаем и сортируем имена используемых колонок
		header := make([]string, 0)
		for columnName := range usedColumns {
			header = append(header, columnName)
		}
		fmt.Println("###################################", header, "######################################")
		sort.Strings(header)

		// Если нет используемых колонок, пропускаем файл
		if len(header) == 0 {
			continue
		}

		// Записываем отсортированный заголовок
		writer.Write(header)

		// Создаем отображение для индексов колонок
		columnIndices := make(map[string]int)
		for i, columnName := range header {
			columnIndices[columnName] = i
		}

		// Записываем данные в отсортированном порядке
		for _, data := range v.Dates {
			values := make([]string, len(header))
			hasData := false
			for column, value := range data {
				if usedColumns[column] {
					values[columnIndices[column]] = fmt.Sprint(value)
					hasData = true
				}
			}
			if hasData {
				writer.Write(values)
			}
		}

		writer.Flush()
		result[name] = buffer.String()
	}

	return result
}

func ZipArchive(csvs map[string]string) []byte {
	var buffer bytes.Buffer
	zipWriter := zip.NewWriter(&buffer)

	// Получаем и сортируем имена файлов
	fileNames := make([]string, 0)
	for name := range csvs {
		fileNames = append(fileNames, name)
	}
	sort.Strings(fileNames)

	// Записываем файлы в отсортированном порядке
	for _, name := range fileNames {
		writer, err := zipWriter.Create(fmt.Sprintf("%s.csv", name))
		if err != nil {
			return nil
		}

		_, err = writer.Write([]byte(csvs[name]))
		if err != nil {
			return nil
		}
	}

	zipWriter.Close()
	return buffer.Bytes()
}
