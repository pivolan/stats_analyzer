// time_series_processor.go
package main

import (
	"encoding/csv"
	"fmt"
	"sort"
	"strings"
	"time"
)

type TimeSeriesData struct {
	Period    string
	Data      []map[string]interface{}
	StartDate time.Time
	EndDate   time.Time
	RowCount  int
	FileName  string
}

// analyzeTimeSeriesFiles анализирует файлы с временными рядами
func analyzeTimeSeriesFiles(stats map[string]CommonStat) map[string]*TimeSeriesData {
	result := make(map[string]*TimeSeriesData)

	for name, stat := range stats {
		if !strings.HasPrefix(name, "dates_") || len(stat.Dates) == 0 {
			continue
		}

		// Извлекаем период из имени
		parts := strings.Split(name, "_")
		period := parts[len(parts)-1]

		data := &TimeSeriesData{
			Period:   period,
			Data:     stat.Dates,
			RowCount: len(stat.Dates),
			FileName: name,
		}

		// Определяем временной диапазон
		for i, row := range stat.Dates {
			if dt, ok := row["datetime"].(string); ok {
				date, _, _, err := tryParseDateTime(dt)
				if err == nil {
					if i == 0 || date.Before(data.StartDate) {
						data.StartDate = date
					}
					if i == 0 || date.After(data.EndDate) {
						data.EndDate = date
					}
				}
			}
		}

		result[period] = data
	}

	return result
}

// selectGraphData выбирает данные для графиков с новой логикой
// selectGraphData выбирает данные для графиков
func selectGraphData(timeSeriesData map[string]*TimeSeriesData) (*TimeSeriesData, *TimeSeriesData) {
	// Определяем порядок периодов
	periodOrder := map[string]int{
		"year":  1,
		"month": 2,
		"day":   3,
		"hour":  4,
	}

	// Сортируем периоды
	periods := make([]string, 0, len(timeSeriesData))
	for period := range timeSeriesData {
		periods = append(periods, period)
	}
	sort.Slice(periods, func(i, j int) bool {
		return periodOrder[periods[i]] < periodOrder[periods[j]]
	})

	var graph1, graph2 *TimeSeriesData
	var lastValidData *TimeSeriesData

	// Выбор данных для первого графика (сверху вниз)
	for _, period := range periods {
		data := timeSeriesData[period]
		if data.RowCount > 10 {
			graph1 = data
			break
		}
	}

	// Выбор данных для второго графика (снизу вверх)
	for i := len(periods) - 1; i >= 0; i-- {
		data := timeSeriesData[periods[i]]

		// Пропускаем, если больше 10000 строк
		if data.RowCount > 10000 {
			continue
		}

		// Если текущий график с менее чем 10 строками, возьмем предыдущий
		if data.RowCount < 10 && lastValidData != nil {
			graph2 = lastValidData
			break
		}

		graph2 = data
		break
	}

	return graph1, graph2
}

// prepareTimeSeriesCSV подготавливает CSV данные для графика
func prepareTimeSeriesCSV(data *TimeSeriesData) string {
	var builder strings.Builder
	writer := csv.NewWriter(&builder)

	// Записываем заголовки
	headers := []string{"datetime", "cnt", "common"}
	writer.Write(headers)

	// Сортируем данные по времени
	sort.Slice(data.Data, func(i, j int) bool {
		dt1, _ := time.Parse("2006-01-02 15:04:05", data.Data[i]["datetime"].(string))
		dt2, _ := time.Parse("2006-01-02 15:04:05", data.Data[j]["datetime"].(string))
		return dt1.Before(dt2)
	})

	// Записываем данные
	for _, row := range data.Data {
		record := []string{
			fmt.Sprintf("%v", row["datetime"]),
			fmt.Sprintf("%v", row["cnt"]),
			fmt.Sprintf("%v", row["common"]),
		}
		writer.Write(record)
	}

	writer.Flush()
	return builder.String()
}

// generateTimeSeriesDescription генерирует описание временного ряда
func generateTimeSeriesDescription(data *TimeSeriesData, graphNum int) string {
	return fmt.Sprintf(`График %d:
Тип группировки: %s
Количество точек: %d
Период: с %s по %s
`,
		graphNum,
		data.Period,
		data.RowCount,
		data.StartDate.Format("2006-01-02 15:04"),
		data.EndDate.Format("2006-01-02 15:04"))
}
