package main

import (
	"bytes"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"github.com/pivolan/go_utils"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func getMD5String(input string) string {
	hasher := md5.New()
	hasher.Write([]byte(input))
	hashBytes := hasher.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)
	return hashString
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
	r := csv.NewReader(f)
	//header
	headers, err := r.Read()
	//headers := []string{"id", "time", "project", "user", "type", "article", "audiofile", "articletype", "progress"}
	typesPriority := []string{"", "String", "Float64", "Int64", "Date"}
	values, err := r.Read()
	types := make([]string, len(values))
	nullables := make([]string, len(values))

	for u := 0; u < 50000; u++ {
		values, err := r.Read()
		if err != nil {
			break
		}
		for n, value := range values {
			f := ""
			var v interface{}
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
			if sort.SearchStrings(typesPriority, t) > sort.SearchStrings(typesPriority, types[n]) {
				types[n] = t
			}
		}
	}
	f.Seek(0, 0)
	r = csv.NewReader(f)
	headers, err = r.Read()
	for i, header := range headers {
		headers[i] = replaceSpecialSymbols(header)
	}
	fmt.Println(headers)
	fmt.Println(types)
	dsn := "default:@tcp(127.0.0.1:9004)/default"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		log.Fatalln("cannot connect to clickhouse", err)
	}

	replacer := strings.NewReplacer(
		" ", "",
		".", "",
		"_", "",
	)
	fields := []string{}
	columns := []string{}
	for i, header := range headers {
		if types[i] == "" {
			types[i] = "String"
		}
		fields = append(fields, fmt.Sprintf("%s %s %s", replacer.Replace(header), types[i], nullables[i]))
		columns = append(columns, replacer.Replace(header))
	}
	tableName := strings.Join(columns, "_") + "_" + getMD5String(filePath)[:6]
	if len(columns) > 2 {
		tableName = strings.Join(columns[:3], "_") + "_" + getMD5String(filePath)[:6]
	}
	fmt.Println("tablename", tableName)

	sql := `CREATE TABLE ` + tableName + ` (id UInt64,`
	//if id already exists
	idExists := false
	for _, v := range headers {
		if v == "id" {
			idExists = true
			sql = `CREATE TABLE ` + tableName + ` (`
		}
	}
	sql += strings.Join(fields, ",\n") + fmt.Sprintf(") ENGINE = ReplacingMergeTree PRIMARY KEY (id) SETTINGS index_granularity = 8192")
	tx := db.Exec("DROP TABLE IF EXISTS " + tableName)
	fmt.Println(sql)
	if tx.Error != nil {
		log.Println("create table error", err)
		return "", tx.Error
	}
	tx = db.Exec(sql)
	if tx.Error != nil {
		return "", tx.Error
	}
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
		if i%10000 == 0 {
			//save
			csvWriter.Flush()
			sql := fmt.Sprintf("INSERT INTO "+tableName+" FORMAT CSV \n%s", b.String())
			//fmt.Println(b.String())
			b.Reset()
			tx = db.Exec(sql)
			if err != nil {
				return "", err
			}
			if tx.Error != nil {
				log.Println(tx.Error)
				return "", tx.Error
			}
		}
	}
	if b.Len() > 0 {
		csvWriter.Flush()
		sql := fmt.Sprintf("INSERT INTO "+tableName+" FORMAT CSV \n%s", b.String())
		fmt.Println(b.String())
		db.Exec(sql)
	}
	fmt.Println("all saved, lines saved:", i)
	return tableName, nil
}

type ColumnInfo struct {
	Name string
	Type string
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

func generateQueries(tableName string, columns []ColumnInfo) []QueryResult {
	var queries []QueryResult

	for _, col := range columns {
		if strings.HasPrefix(col.Type, "Int") || strings.HasPrefix(col.Type, "Float") {
			query := QueryResult{
				Sql:          fmt.Sprintf("SELECT, median(%[1]s) AVG(%[1]s), MAX(%[1]s), MIN(%[1]s), SUM(%[1]s) FROM %s", col.Name, tableName),
				SingleOrMany: RESULT_SINGLE,
			}
			queries = append(queries, query)

		} else if strings.HasPrefix(col.Type, "Date") || strings.HasPrefix(col.Type, "DateTime") {
		} else {
		}
	}

	return queries
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
	Uniq                                            int64
	Avg, Min, Max, Median, Quantile001, Quantile099 float64
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
		v, ok := value.(int64)
		if !ok {
			return fmt.Errorf("value %v is not of type int64", value)
		}
		fieldValue.SetInt(v)
	case reflect.Float64:
		v, ok := value.(float64)
		if !ok {
			return fmt.Errorf("value %v is not of type float64", value)
		}
		fieldValue.SetFloat(v)
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
func TestZero(t *testing.T) {
	a := CommonStat{Avg: -2.23}
	b := CommonStat{Avg: 5, Min: 7.5, Uniq: 58}
	setZeroFields(&a, b)
	fmt.Println(a, b)
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

func generateSqlForNumericColumnsStats(columns []ColumnInfo, table string) (sql string) {
	statMethods := []string{"quantile(0.01)", "quantile(0.99)", "median", "avg", "max", "min"}
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
	return "SELECT " + strings.Join(fields, ",") + " FROM " + table
}
