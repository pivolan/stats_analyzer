package main

import (
	"bytes"
	"crypto/md5"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"regexp"
	"sort"
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
	tableName := getMD5String(filePath)
	fmt.Println("tablename", tableName)
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

	sql := `CREATE TABLE ` + tableName + ` (id UInt64,`
	//if id already exists
	idExists := false
	for _, v := range headers {
		if v == "id" {
			idExists = true
			sql = `CREATE TABLE ` + tableName + ` (`
		}
	}

	replacer := strings.NewReplacer(
		" ", "",
		".", "",
		"_", "",
	)
	fields := []string{}
	for i, header := range headers {
		if types[i] == "" {
			types[i] = "String"
		}
		fields = append(fields, fmt.Sprintf("%s %s %s", replacer.Replace(header), types[i], nullables[i]))
	}
	sql += strings.Join(fields, ",\n") + fmt.Sprintf(") ENGINE = ReplacingMergeTree PRIMARY KEY (id) SETTINGS index_granularity = 8192")
	tx := db.Exec("DROP TABLE IF EXISTS " + tableName)
	fmt.Println(sql)
	if err != nil {
		log.Println("create table error", err)
		return "", err
	}
	tx = db.Exec(sql)
	if err != nil {
		return "", err
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
			fmt.Println(b.String())
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
