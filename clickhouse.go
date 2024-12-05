package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/pivolan/go_utils"
	"gorm.io/gorm"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

func getMD5String(input string) string {
	hasher := md5.New()
	hasher.Write([]byte(input))
	hashBytes := hasher.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)
	return hashString
}
func SearchStrings(a []string, x string) int {
	for i, s := range a {
		if s == x {
			return i
		}
	}
	return -1
}
func replaceSpecialSymbols(input string) string {
	// Replace all non-alphanumeric characters with underscores
	re := regexp.MustCompile("[^a-zA-Z0-9]+")
	processedString := re.ReplaceAllString(input, "_")

	// Replace any consecutive underscores with a single underscore
	processedString = strings.ReplaceAll(processedString, "__", "_")

	// Remove any underscores at the beginning or end of the string
	processedString = strings.Trim(processedString, "_")

	return processedString
}

type ColumnInfo struct {
	Name string
	Type string //Date DateTime64 Int64 Float64
}

func getColumnAndTypeList(db *gorm.DB, tableName string) ([]ColumnInfo, error) {
	query := fmt.Sprintf("DESCRIBE TABLE %s", tableName)
	tx := db.Raw(query)
	if tx.Error != nil {
		return nil, tx.Error
	}

	var columns []ColumnInfo
	tx.Scan(&columns)

	return columns, nil
}

type StatsQueryResultType string

const RESULT_MANY StatsQueryResultType = "many"
const RESULT_SINGLE StatsQueryResultType = "single"

type QueryResult struct {
	Sql          string
	SingleOrMany StatsQueryResultType
}

func IsNumericType(_type string) bool {
	return go_utils.InArray(_type, []string{"Int64", "Float64", "Nullable(Int64)", "Nullable(Float64)"})
}
func RemoveSpecialChars(s string) string {
	// Initialize a new buffer to hold the cleaned string
	var buf bytes.Buffer

	// Loop over the input string, and only add letters and numbers to the buffer
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			buf.WriteRune(c)
		}
	}

	// Return the cleaned string as a string
	return buf.String()
}

type CommonStat struct {
	Count                                                                   int64
	Uniq                                                                    int64
	Avg, Min, Max, Median, Quantile001, Quantile01, Quantile099, Quantile09 float64
	Dates                                                                   []map[string]interface{}
	Groups                                                                  []map[string]interface{}
}

func (c *CommonStat) Set(key string, value interface{}) error {
	// Build name of struct field based on key
	fieldName := strings.Title(key)

	// Get value of struct field based on fieldName using reflection
	fieldValue := reflect.ValueOf(c).Elem().FieldByName(fieldName)

	// Check if field exists and is settable
	if !fieldValue.IsValid() || !fieldValue.CanSet() {
		return fmt.Errorf("field %s not found or not settable", fieldName)
	}

	// Check if value is of correct type
	switch fieldValue.Kind() {
	case reflect.Int64:
		var result int64
		switch v := value.(type) {
		case float64:
			fmt.Println("value is a float64")
			result = int64(v)
		case int64:
			fmt.Println("value is an int")
			result = int64(v)
		case string:
			fmt.Println("value is a string")
			// Convert value to int64
			if val, err := strconv.ParseInt(v, 10, 64); err == nil {
				result = val
			} else {
				fmt.Println("Unable to convert string to int64")
			}
		default:
			fmt.Println("value is not a number or string")
		}
		fieldValue.SetInt(result)
	case reflect.Float64:
		var result float64

		switch v := value.(type) {
		case float64:
			result = v
		case int64:
			result = float64(v)
		case string:
			// Convert x to float64
			if val, err := strconv.ParseFloat(v, 64); err == nil {
				result = val
			} else {
				fmt.Println("Unable to convert string to float64")
			}
		default:
			fmt.Println("x is not a number or string")
		}
		fieldValue.SetFloat(result)
	default:
		return fmt.Errorf("field %s is not of type int64 or float64", fieldName)
	}

	return nil
}

func parseUniqResults(uniqResults map[string]interface{}) (result map[string]CommonStat) {
	result = map[string]CommonStat{}
	for field, value := range uniqResults {
		result[field] = CommonStat{Uniq: value.(int64)}
	}
	return
}
func parseCountResults(countResults map[string]interface{}) (result map[string]CommonStat) {
	result = map[string]CommonStat{}
	for _, value := range countResults {
		result["all"] = CommonStat{Count: value.(int64)}
	}
	return
}
func parseNumericResults(param map[string]interface{}) (result map[string]CommonStat) {
	result = map[string]CommonStat{}
	for param, value := range param {
		args := strings.Split(param, "__")
		if len(args) != 2 {
			continue
		}
		method := args[0]
		field := args[1]
		if _, ok := result[field]; !ok {
			result[field] = CommonStat{}
		}
		stat := result[field]
		stat.Set(method, value)
		result[field] = stat
	}
	return
}
func mergeStat(results ...map[string]CommonStat) (response map[string]CommonStat) {
	response = map[string]CommonStat{}
	for _, result := range results {
		for field, values := range result {
			_ = values
			if _, ok := response[field]; !ok {
				response[field] = CommonStat{}
			}
			stat := response[field]
			setZeroFields(&stat, values)
			response[field] = stat
		}
	}
	return
}
func setZeroFields(a *CommonStat, b CommonStat) {
	// Get type and value of struct a using reflection
	aType := reflect.TypeOf(a).Elem()
	aValue := reflect.ValueOf(a).Elem()

	// Iterate through fields of struct a
	for i := 0; i < aType.NumField(); i++ {
		field := aType.Field(i)
		fieldValue := aValue.Field(i)

		// Check if field is zero
		switch fieldValue.Kind() {
		case reflect.Int64:
			if fieldValue.Int() == 0 {
				// Get corresponding field in struct b using reflection
				bFieldValue := reflect.ValueOf(&b).Elem().FieldByName(field.Name)

				// If field in struct b is not zero, set field in struct a to that value
				if bFieldValue.Int() != 0 {
					fieldValue.SetInt(bFieldValue.Int())
				}
			}
		case reflect.Float64:
			if fieldValue.Float() == 0 {
				// Get corresponding field in struct b using reflection
				bFieldValue := reflect.ValueOf(&b).Elem().FieldByName(field.Name)

				// If field in struct b is not zero, set field in struct a to that value
				if bFieldValue.Float() != 0 {
					fieldValue.SetFloat(bFieldValue.Float())
				}
			}
		}
	}
}

func excludeColumn(name string) bool {
	return go_utils.InArray(name, []string{"id", "slug"})
}
func generateSqlForUniqCounts(columns []ColumnInfo, table string) (sql string) {
	fields := []string{}
	method := "uniq"
	for _, column := range columns {
		if excludeColumn(column.Name) {
			continue
		}
		fields = append(fields, fmt.Sprintf("%s(%s) as %s", method, column.Name, column.Name))
	}
	return "SELECT " + strings.Join(fields, ",") + " FROM " + table
}
func generateSqlForCount(columns []ColumnInfo, table string) (sql string) {
	return "SELECT count() FROM " + table
}

func generateSqlForNumericColumnsStats(columns []ColumnInfo, table string) (sql string) {
	statMethods := []string{"quantile(0.01)", "quantile(0.99)", "median", "avg", "max", "min"}
	fields := columnAggregatesSelectSqlGenerator(columns, statMethods)
	return "SELECT " + strings.Join(fields, ",") + " FROM " + table
}
func columnAggregatesSelectSqlGenerator(columns []ColumnInfo, aggregatesMethods []string) []string {
	statMethods := aggregatesMethods
	fields := []string{}
	for _, column := range columns {
		if excludeColumn(column.Name) {
			continue
		}
		if IsNumericType(column.Type) {
			for _, method := range statMethods {
				methodEscaped := RemoveSpecialChars(method)
				fields = append(fields, fmt.Sprintf("%s(%s) as %s__%s", method, column.Name, methodEscaped, column.Name))
			}
		}
	}
	return fields
}
