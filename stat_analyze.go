package main

import (
	"fmt"
	"github.com/pivolan/stats_analyzer/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"strings"
)

func analyzeStatistics(tableName ClickhouseTableName) map[string]CommonStat {
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		log.Fatalln("cannot connect to clickhouse", err)
	}

	columnsInfo, err := getColumnAndTypeList(db, tableName)
	fmt.Println(columnsInfo, err)
	sql1 := generateSqlForNumericColumnsStats(columnsInfo, tableName)
	fmt.Println(sql1)
	sql2 := generateSqlForUniqCounts(columnsInfo, tableName)
	fmt.Println(sql2)
	sql3 := generateSqlForCount(columnsInfo, tableName)
	fmt.Println(sql3)
	numericInfo := map[string]interface{}{}
	tx := db.Raw(sql1)
	tx.Scan(numericInfo)
	fmt.Println(numericInfo)
	uniqInfo := map[string]interface{}{}
	tx2 := db.Raw(sql2)
	tx2.Scan(uniqInfo)
	countInfo := map[string]interface{}{}
	tx3 := db.Raw(sql3)
	tx3.Scan(countInfo)
	r1 := parseUniqResults(uniqInfo)
	r2 := parseNumericResults(numericInfo)
	r3 := parseCountResults(countInfo)
	r := mergeStat(r1, r2, r3)
	//generate by date fields
	sqls3 := generateSqlForGroupByDates(columnsInfo, tableName)
	for name, sql3 := range sqls3 {
		fmt.Println(sql3)
		dateAggregatesInfo := []map[string]interface{}{}

		tx3 := db.Raw(sql3)
		t := tx3.Scan(&dateAggregatesInfo)
		if t.Error != nil {
			fmt.Println(t.Error)
		}
		datesInfo := CommonStat{Dates: dateAggregatesInfo}
		r[fmt.Sprintf("dates_%s", name)] = datesInfo
	}
	//groups
	sqls4 := generateSqlForGroups(columnsInfo, r1, tableName)
	for _, line := range sqls4 {
		info := strings.Split(line, "//")
		columnName := info[0]
		title := info[1]
		sql := info[2]
		fmt.Println(info)
		dateAggregatesInfo := []map[string]interface{}{}

		tx3 := db.Raw(sql)
		t := tx3.Scan(&dateAggregatesInfo)
		if t.Error != nil {
			fmt.Println(t.Error)
		}
		groupsInfo := r[columnName]
		groupsInfo.Groups = dateAggregatesInfo
		groupsInfo.Title = title
		r[columnName] = groupsInfo
	}
	return r
}

func generateSqlForGroups(columnInfos []ColumnInfo, uniqInfos map[string]CommonStat, table ClickhouseTableName) []string {
	sqls := []string{}
	groupedColumns := []string{}
	for _, columnInfo := range columnInfos {
		if columnInfo.Type == "String" || columnInfo.Type == "Nullable(String)" {
			if uniqInfo, ok := uniqInfos[columnInfo.Name]; ok {
				if uniqInfo.Uniq > 1 && uniqInfo.Uniq < 1000 {
					sql1 := columnInfo.Name + "//Самые частые//SELECT count(*), " + columnInfo.Name + " FROM " + string(table) + " GROUP BY " + columnInfo.Name + " ORDER BY count(*) DESC LIMIT 100"
					sqls = append(sqls, sql1)
					//if uniqInfo.Uniq > 100 {
					//	sql2 := columnInfo.Name + "//Редкие//SELECT count(*), " + columnInfo.Name + " FROM " + string(table) + " GROUP BY " + columnInfo.Name + " ORDER BY count(*) LIMIT 100"
					//	sqls = append(sqls, sql2)
					//}
					groupedColumns = append(groupedColumns, columnInfo.Name)
				}
			}
		}
	}
	if len(groupedColumns) > 1 {
		sql1 := "9999_all_columns_frequently//Группировка по всем колонкам самые частые варианты//SELECT count(*), " + strings.Join(groupedColumns, ",") + " FROM " + string(table) + " GROUP BY " + strings.Join(groupedColumns, ",") + " ORDER BY count(*) DESC LIMIT 100"
		sql2 := "9999_all_columns_rarely//Группировка по всем колонкам самые редкие//SELECT count(*), " + strings.Join(groupedColumns, ",") + " FROM " + string(table) + " GROUP BY " + strings.Join(groupedColumns, ",") + " ORDER BY count(*) LIMIT 100"
		sqls = append(sqls, sql1, sql2)
	}
	return sqls
}

// method will find all date fields and generate sql for group by them
func generateSqlForGroupByDates(columnsInfo []ColumnInfo, table ClickhouseTableName) map[string]string {
	sqls := map[string]string{}
	for _, columnInfo := range columnsInfo {
		truncateDatesList := []string{"year", "month", "day"}
		if strings.HasPrefix(columnInfo.Type, "DateTime") {
			truncateDatesList = append(truncateDatesList, "hour")
		}
		if strings.HasPrefix(columnInfo.Type, "Date") {
			fields := columnAggregatesSelectSqlGenerator(columnsInfo, []string{"min", "max", "median", "avg"})

			for _, truncDate := range truncateDatesList {
				sql := "SELECT toString(date_trunc('" + truncDate + "', " + columnInfo.Name + ")) as datetime, count(*) as cnt, 'info' as common, " + strings.Join(fields, ",") +
					" FROM " + string(table) + " GROUP BY datetime ORDER BY 1 DESC"
				sqls[fmt.Sprintf("%s_%s", columnInfo.Name, truncDate)] = sql
			}
		}
	}

	//select trunc('hour', column), count(*), min(column1), max(column2), ... from table_name group by 1 order by 1 desc
	return sqls
}
