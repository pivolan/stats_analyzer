package main

import (
	"bytes"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"github.com/pivolan/go_utils"
	"github.com/pivolan/stats_analyzer/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
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

func importDataIntoClickHouse(filePath string) (string, error) {
	sourceCSV := filePath
	f, err := os.OpenFile(sourceCSV, os.O_RDONLY, 0655)
	if err != nil {
		return "", err
	}
	defer f.Close()

	r := csv.NewReader(f)

	// Читаем и анализируем первую строку
	firstRow, err := r.Read()
	if err != nil {
		return "", err
	}

	// Анализируем заголовки
	headerAnalysis := AnalyzeHeaders(firstRow)
	if headerAnalysis == nil {
		return "", fmt.Errorf("empty CSV file")
	}

	// Проверяем и валидируем заголовки
	headers := ValidateHeaders(headerAnalysis.Headers)

	// Получаем первую строку данных
	var dataRow []string
	if headerAnalysis.FirstRowIsData {
		dataRow = headerAnalysis.FirstDataRow
	} else {
		dataRow, err = r.Read()
		if err != nil {
			return "", err
		}
	}

	// Определяем типы данных
	typesWeight := []string{"", "DateTime64", "Date", "Int64", "Float64", "String"}
	types := make([]string, len(dataRow))
	nullables := make([]string, len(dataRow))

	// Анализируем типы, начиная с первой строки данных
	for i := 0; i < 50000; i++ {
		var values []string
		if i == 0 {
			values = dataRow
		} else {
			var err error
			values, err = r.Read()
			if err != nil {
				break
			}
		}

		for n, value := range values {
			f := ""
			var v interface{}
			v, err = time.Parse("2006-01-02 15:04:05.999999", value)
			if err == nil {
				f = "DateTime"
			}
			if err != nil {
				v, err = time.Parse("2006-01-02 15:04:05", value)
				if err == nil {
					f = "DateTime"
				}
				if err != nil {
					v, err = time.Parse("2006-01-02", value)
					if err != nil {
						v, err = strconv.ParseUint(value, 10, 64)
						if err != nil {
							v, err = strconv.ParseInt(value, 10, 64)
							if err != nil {
								v, err = strconv.ParseFloat(value, 64)
								if err != nil {
									v = value
								}
							}
						}
					}
				}
			}

			t := ""
			switch v.(type) {
			case time.Time:
				if f == "DateTime" {
					t = "DateTime64"
				} else {
					t = "Date"
				}
			case uint64:
				t = "Int64"
			case int64:
				t = "Int64"
			case float64:
				t = "Float64"
			case string:
				if v == "" {
					nullables[n] = " NULL "
					continue
				}
				t = "String"
			}
			currentTypeWeight := SearchStrings(typesWeight, t)
			savedTypeWeight := SearchStrings(typesWeight, types[n])
			if currentTypeWeight > savedTypeWeight {
				types[n] = t
			}
		}
	}

	// Создаем таблицу
	fields := []string{}
	columns := []string{}
	for i, header := range headers {
		if types[i] == "" {
			types[i] = "String"
		}
		fields = append(fields, fmt.Sprintf("%s %s %s", header, types[i], nullables[i]))
		columns = append(columns, header)
	}

	// Генерируем имя таблицы
	tableName := strings.Join(columns[:min(3, len(columns))], "_") + "_" + getMD5String(filePath)[:6]

	// Создаем SQL запрос
	sql := `CREATE TABLE ` + tableName + ` (id UInt64,`
	idExists := false
	for _, v := range headers {
		if v == "id" {
			idExists = true
			sql = `CREATE TABLE ` + tableName + ` (`
		}
	}
	sql += strings.Join(fields, ",\n") + fmt.Sprintf(") ENGINE = ReplacingMergeTree PRIMARY KEY (id) SETTINGS index_granularity = 8192")

	// Подключаемся к базе данных
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DbDsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return "", err
	}

	// Создаем таблицу
	tx := db.Exec("DROP TABLE IF EXISTS " + tableName)
	if tx.Error != nil {
		return "", tx.Error
	}
	tx = db.Exec(sql)
	if tx.Error != nil {
		return "", tx.Error
	}

	// Возвращаемся к началу файла для импорта
	f.Seek(0, 0)
	r = csv.NewReader(f)

	// Пропускаем заголовки, если они не являются данными
	if !headerAnalysis.FirstRowIsData {
		_, _ = r.Read()
	}

	// Импортируем данные
	b := bytes.NewBufferString("")
	csvWriter := csv.NewWriter(b)
	i := 0
	for ; ; i++ {
		values, err := r.Read()
		if err != nil {
			break
		}
		for k, v := range values {
			if types[k] == "String" || types[k] == "Date" || types[k] == "DateTime" {
				values[k] = "'" + v + "'"
			}
		}

		if !idExists {
			values = append([]string{strconv.Itoa(i)}, values...)
		}
		csvWriter.Write(values)

		if i%5000 == 0 {
			csvWriter.Flush()
			sql := fmt.Sprintf("INSERT INTO "+tableName+" FORMAT CSV \n%s", b.String())
			b.Reset()
			tx = db.Exec(sql)
			if tx.Error != nil {
				return "", tx.Error
			}
		}
	}

	if b.Len() > 0 {
		csvWriter.Flush()
		sql := fmt.Sprintf("INSERT INTO "+tableName+" FORMAT CSV \n%s", b.String())
		tx = db.Exec(sql)
		if tx.Error != nil {
			return "", tx.Error
		}
	}

	return tableName, nil
}

// Вспомогательная функция для определения минимального значения
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
