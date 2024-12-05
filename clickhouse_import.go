package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const SEPARATOR = ','

func importDataIntoClickHouse(filePath string) (string, error) {
	sourceCSV := filePath
	f, err := os.OpenFile(sourceCSV, os.O_RDONLY, 0655)
	if err != nil {
		return "", err
	}
	r := csv.NewReader(f)
	//header
	r.Comma = SEPARATOR
	r.LazyQuotes = true
	headers, err := r.Read()
	//headers := []string{"id", "time", "project", "user", "type", "article", "audiofile", "articletype", "progress"}
	typesWeight := []string{"", "DateTime64", "Date", "Int64", "Float64", "String"}
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
	f.Seek(0, 0)
	r = csv.NewReader(f)
	r.Comma = SEPARATOR
	r.LazyQuotes = true
	headers, err = r.Read()
	for i, header := range headers {
		headers[i] = replaceSpecialSymbols(header)
	}
	fmt.Println(headers)
	fmt.Println(types)
	db, err := gorm.Open(mysql.Open(DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
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
			fmt.Println("csv read err", err)
			break
		}
		for k, v := range values {
			if types[k] == "String" || types[k] == "Date" || types[k] == "DateTime" {
				values[k] = "'" + replaceSpecialSymbols(v) + "'"
			}
		}

		if !idExists {
			values = append([]string{strconv.Itoa(i)}, values...)
		}
		csvWriter.Write(values)
		if i%5000 == 0 {
			//save
			csvWriter.Flush()
			sql := fmt.Sprintf("INSERT INTO "+tableName+" FORMAT CSV \n%s", b.String())
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
	csvWriter.Flush()
	if b.Len() > 0 {
		sql := fmt.Sprintf("INSERT INTO "+tableName+" FORMAT CSV \n%s", b.String())
		db.Exec(sql)
	}
	fmt.Println("all saved, lines saved:", i)
	return tableName, nil
}
