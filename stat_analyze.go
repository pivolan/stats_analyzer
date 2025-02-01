package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/pivolan/stats_analyzer/config"
	"github.com/pivolan/stats_analyzer/domain/models"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func analyzeStatistics(tableName models.ClickhouseTableName) map[string]CommonStat {
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

	numericCOl := findNumColomn(db, tableName)

	sqls5 := generateSqlForOneGraphByDates(columnsInfo, numericCOl, tableName)

	for name, sqls5 := range sqls5 {
		fmt.Println(sqls5)
		dateAggregatesInfo := []map[string]interface{}{}

		tx5 := db.Raw(sqls5)
		t := tx5.Scan(&dateAggregatesInfo)
		if t.Error != nil {
			fmt.Println(t.Error)
		}
		datesInfo := CommonStat{Dates: dateAggregatesInfo}
		r[fmt.Sprintf("graph_%s", name)] = datesInfo

	}

	return r
}
func generateSqlForGroups(columnInfos []models.ColumnInfo, uniqInfos map[string]CommonStat, table models.ClickhouseTableName) []string {
	sqls := []string{}
	groupedColumns := []string{}

	for _, columnInfo := range columnInfos {
		if columnInfo.Type == "String" || columnInfo.Type == "Nullable(String)" {
			if uniqInfo, ok := uniqInfos[columnInfo.Name]; ok {
				if uniqInfo.Uniq > 1 && uniqInfo.Uniq < 1000 {
					// Modified SQL to include percentage and better formatting
					mostFrequentSQL := fmt.Sprintf(`%s//Самые частые значения в %s//
                        SELECT 
                            count(*) as count,
                            %s as value,
                            (count(*) * 100.0 / (SELECT count(*) FROM %s)) as percentage
                        FROM %s 
                        WHERE %s IS NOT NULL
                        GROUP BY %s 
                        ORDER BY count DESC 
                        LIMIT 10`,
						columnInfo.Name, columnInfo.Name,
						columnInfo.Name, table, table,
						columnInfo.Name, columnInfo.Name)

					leastFrequentSQL := fmt.Sprintf(`%s_rare//Редкие значения в %s//
                        SELECT 
                            count(*) as count,
                            %s as value,
                            (count(*) * 100.0 / (SELECT count(*) FROM %s)) as percentage
                        FROM %s 
                        WHERE %s IS NOT NULL
                        GROUP BY %s 
                        HAVING count(*) > 1
                        ORDER BY count ASC 
                        LIMIT 10`,
						columnInfo.Name, columnInfo.Name,
						columnInfo.Name, table, table,
						columnInfo.Name, columnInfo.Name)

					sqls = append(sqls, mostFrequentSQL, leastFrequentSQL)
					groupedColumns = append(groupedColumns, columnInfo.Name)
				}
			}
		}
	}

	// Modified combined columns analysis
	if len(groupedColumns) > 1 {
		combinedFrequentSQL := fmt.Sprintf(`9999_all_columns_frequently//Частые комбинации значений//
            SELECT 
                count(*) as count,
                %s,
                (count(*) * 100.0 / (SELECT count(*) FROM %s)) as percentage
            FROM %s 
            GROUP BY %s 
            ORDER BY count DESC 
            LIMIT 10`,
			strings.Join(groupedColumns, ","),
			table, table,
			strings.Join(groupedColumns, ","))

		combinedRareSQL := fmt.Sprintf(`9999_all_columns_rarely//Редкие комбинации значений//
            SELECT 
                count(*) as count,
                %s,
                (count(*) * 100.0 / (SELECT count(*) FROM %s)) as percentage
            FROM %s 
            GROUP BY %s 
            HAVING count(*) > 1
            ORDER BY count ASC 
            LIMIT 10`,
			strings.Join(groupedColumns, ","),
			table, table,
			strings.Join(groupedColumns, ","))

		sqls = append(sqls, combinedFrequentSQL, combinedRareSQL)
	}

	return sqls
}

// Add a new function to format the results nicely
func formatValueFrequency(groups []map[string]interface{}) string {
	var result strings.Builder

	for _, group := range groups {
		count := group["count"].(int64)
		value := group["value"].(string)
		percentage := group["percentage"].(float64)

		result.WriteString(fmt.Sprintf("• %s (%d раз, %.2f%%)\n",
			value, count, percentage))
	}

	return result.String()
}

// func generateSqlForGroups(columnInfos []models.ColumnInfo, uniqInfos map[string]CommonStat, table models.ClickhouseTableName) []string {
// 	sqls := []string{}
// 	groupedColumns := []string{}
// 	for _, columnInfo := range columnInfos {
// 		if columnInfo.Type == "String" || columnInfo.Type == "Nullable(String)" {
// 			if uniqInfo, ok := uniqInfos[columnInfo.Name]; ok {
// 				if uniqInfo.Uniq > 1 && uniqInfo.Uniq < 1000 {
// 					sql1 := columnInfo.Name + "//Самые частые//SELECT count(*), " + columnInfo.Name + " FROM " + string(table) + " GROUP BY " + columnInfo.Name + " ORDER BY count(*) DESC LIMIT 100"
// 					sqls = append(sqls, sql1)
// 					//if uniqInfo.Uniq > 100 {
// 					//	sql2 := columnInfo.Name + "//Редкие//SELECT count(*), " + columnInfo.Name + " FROM " + string(table) + " GROUP BY " + columnInfo.Name + " ORDER BY count(*) LIMIT 100"
// 					//	sqls = append(sqls, sql2)
// 					//}
// 					groupedColumns = append(groupedColumns, columnInfo.Name)
// 				}
// 			}
// 		}
// 	}
// 	if len(groupedColumns) > 1 {
// 		sql1 := "9999_all_columns_frequently//Группировка по всем колонкам самые частые варианты//SELECT count(*), " + strings.Join(groupedColumns, ",") + " FROM " + string(table) + " GROUP BY " + strings.Join(groupedColumns, ",") + " ORDER BY count(*) DESC LIMIT 100"
// 		sql2 := "9999_all_columns_rarely//Группировка по всем колонкам самые редкие//SELECT count(*), " + strings.Join(groupedColumns, ",") + " FROM " + string(table) + " GROUP BY " + strings.Join(groupedColumns, ",") + " ORDER BY count(*) LIMIT 100"
// 		sqls = append(sqls, sql1, sql2)
// 	}
// 	return sqls
// }

// method will find all date fields and generate sql for group by them
func generateSqlForGroupByDates(columnsInfo []models.ColumnInfo, table models.ClickhouseTableName) map[string]string {
	sqls := map[string]string{}
	for _, columnInfo := range columnsInfo {
		truncateDatesList := []string{"hour", "year", "month", "day"}
		if strings.HasPrefix(columnInfo.Type, "DateTime") {
			truncateDatesList = append(truncateDatesList, "hour")
		}
		if strings.HasPrefix(columnInfo.Type, "Date") {
			fields := columnAggregatesSelectSqlGenerator(columnsInfo, []string{"min", "max", "median", "avg"})

			for _, truncDate := range truncateDatesList {
				sql := "SELECT toString(date_trunc('" + truncDate + "', " + columnInfo.Name + ")) as datetime, count(*) as cnt, 'info' as common, " + strings.Join(fields, ",") +
					" FROM " + string(table) + " GROUP BY datetime ORDER BY 1 DESC"
				sqls[fmt.Sprintf("%s__%s", columnInfo.Name, truncDate)] = sql
			}
		}
	}

	//select trunc('hour', column), count(*), min(column1), max(column2), ... from table_name group by 1 order by 1 desc
	return sqls
}

func generateSqlForOneGraphByDates(columnsInfo []models.ColumnInfo, numericColumn string, table models.ClickhouseTableName) map[string]string {
	sqls := map[string]string{}

	// Ищем колонки с типом Date или DateTime
	for _, columnInfo := range columnsInfo {
		if !strings.HasPrefix(columnInfo.Type, "Date") && !strings.HasPrefix(columnInfo.Type, "DateTime") {
			continue
		}

		// Определяем доступные интервалы
		truncateDatesList := []string{"year", "month", "day"}
		if strings.HasPrefix(columnInfo.Type, "DateTime") {
			// Для DateTime добавляем часы в начало списка
			truncateDatesList = append([]string{"hour"}, truncateDatesList...)
		}

		// Генерируем SQL для каждого интервала
		for _, truncdate := range truncateDatesList {
			if numericColumn != "" {
				sql := fmt.Sprintf(`
                    SELECT
                        toString(date_trunc('%s', %s)) as date,
                        count(*) as cnt,
                        sum(%s) as sum_value
                    FROM %s
                    GROUP BY date
                    ORDER BY date
                `, truncdate, columnInfo.Name, numericColumn, table)

				key := fmt.Sprintf("%s__%s", columnInfo.Name, truncdate)
				sqls[key] = sql
			}
		}
	}

	return sqls
}

func findNumColomn(db *gorm.DB, table models.ClickhouseTableName) string {
	var numericColumn string
	numericColSQL := fmt.Sprintf(`
	SELECT name
	FROM system.columns
	WHERE table = '%s'
	AND type IN ('Int8', 'Int16', 'Int32', 'Int64', 'Float32', 'Float64', 'Decimal')
	LIMIT 1`, table)
	err := db.Raw(numericColSQL).Scan(&numericColumn).Error
	if err != nil {
		log.Printf("Error getting numeric column: %v", err)
	}
	return numericColumn
}
