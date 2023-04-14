package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
)

func TestGetColumnAndTypeList(t *testing.T) {
	db, err := gorm.Open(mysql.Open("default:@tcp(127.0.0.1:9004)/default"))
	assert.NoError(t, err)
	tableName := "id_time_project_fa6af7"
	columns, err := getColumnAndTypeList(db, tableName)
	assert.NoError(t, err)
	fmt.Printf("%+v\n", columns)
	sql1 := generateSqlForNumericColumnsStats(columns, tableName)
	fmt.Println(sql1)
	sql2 := generateSqlForUniqCounts(columns, tableName)
	fmt.Println(sql2)
	numericInfo := map[string]interface{}{}
	tx := db.Raw(sql1)
	tx.Scan(numericInfo)
	assert.NoError(t, tx.Error)
	fmt.Println(numericInfo)
	uniqInfo := map[string]interface{}{}
	tx2 := db.Raw(sql2)
	assert.NoError(t, tx2.Error)
	tx2.Scan(uniqInfo)
	fmt.Println(uniqInfo)
	r1 := parseUniqResults(uniqInfo)
	r2 := parseNumericResults(numericInfo)
	r := mergeStat(r1, r2)
	fmt.Println("results 1", r1)
	fmt.Println("results 2", r2)
	fmt.Println("merged", r)
	for key, value := range r {
		fmt.Println(key, value)
	}
}
