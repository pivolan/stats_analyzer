package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/pivolan/stats_analyzer/config"
	"github.com/pivolan/stats_analyzer/domain/models"
	"github.com/pivolan/stats_analyzer/plot"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func handleCommand(api *tgbotapi.BotAPI, update tgbotapi.Update) {
	// Получаем полную команду (без слеша)
	fullCommand := update.Message.Command()

	// Префиксы команд
	graphPrefix := "graph_"
	detailsPrefix := "details_"
	datesPrefix := "dates_"

	// Проверяем и обрабатываем команды по префиксам
	switch {
	case strings.HasPrefix(fullCommand, graphPrefix):
		// Получаем имя колонки, отрезая префикс
		columnName := strings.TrimPrefix(fullCommand, graphPrefix)
		if columnName == "" {
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Укажите имя колонки после graph")
			api.Send(msg)
			return
		}
		handleGraphColumn(api, update, columnName)

	case strings.HasPrefix(fullCommand, detailsPrefix):
		// Получаем имя колонки, отрезая префикс
		columnName := strings.TrimPrefix(fullCommand, detailsPrefix)
		if columnName == "" {
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Укажите имя колонки после details")
			api.Send(msg)
			return
		}

		handleColumnDetails(api, update, columnName)
	case strings.HasPrefix(fullCommand, datesPrefix):
		// Получаем имя колонки, отрезая префикс
		columnName := strings.TrimPrefix(fullCommand, datesPrefix)
		if columnName == "" {
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Укажите имя колонки после dates")
			api.Send(msg)
			return
		}
		handleColumnDates(api, update, columnName)
	case fullCommand == "start":
		handleStartCommand(api, update)
	default:
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Неизвестная команда. Используйте: /graph<имя_колонки> или /details<имя_колонки>")
		api.Send(msg)
	}
}

func handleColumnDates(api *tgbotapi.BotAPI, update tgbotapi.Update, columnName string) {
	tableName, exists := currentTable[update.Message.Chat.ID]
	if !exists {
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Сначала выберите таблицу")
		api.Send(msg)
		return
	}

	// Parse column name to extract base field and time unit
	parts := strings.Split(columnName, "__")
	if len(parts) != 2 {
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Неверный формат имени колонки. Ожидается: field__timeunit")
		api.Send(msg)
		return
	}

	baseField := parts[0]
	timeUnit := parts[1]

	// Connect to database
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Printf("Error connecting to database: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка подключения к базе данных")
		api.Send(msg)
		return
	}

	// Prepare date_trunc expression based on time unit
	dateTruncExpr := ""
	switch timeUnit {
	case "hour":
		dateTruncExpr = fmt.Sprintf("date_trunc('hour', %s)", baseField)
	case "day":
		dateTruncExpr = fmt.Sprintf("date_trunc('day', %s)", baseField)
	case "week":
		dateTruncExpr = fmt.Sprintf("date_trunc('week', %s)", baseField)
	case "month":
		dateTruncExpr = fmt.Sprintf("date_trunc('month', %s)", baseField)
	case "year":
		dateTruncExpr = fmt.Sprintf("date_trunc('year', %s)", baseField)
	default:
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Неподдерживаемая единица времени. Используйте: hour, day, week, month или year")
		api.Send(msg)
		return
	}

	// Single SQL query to get grouped date counts
	dateSQL := fmt.Sprintf(`
        SELECT
            toString(%s) as date,
            count(*) as count
        FROM %s
        WHERE %s IS NOT NULL
        GROUP BY %s
        ORDER BY %s
    `, dateTruncExpr, tableName, baseField, dateTruncExpr, dateTruncExpr)

	var dateCounts []models.DateCount
	if err := db.Raw(dateSQL).Scan(&dateCounts).Error; err != nil {
		log.Printf("Error getting date counts: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка получения статистики по датам")
		api.Send(msg)
		return
	}

	// Добавляем отладочный вывод
	log.Printf("Raw SQL query: %s", dateSQL)
	log.Printf("Number of results: %d", len(dateCounts))
	for i, dc := range dateCounts {
		log.Printf("Row %d: date = %s, count = %d", i, dc.Date, dc.Count)
	}

	// Также проверим значения после парсинга
	xValues := make([]float64, 0, len(dateCounts))
	yValues := make([]float64, 0, len(dateCounts))

	const (
		fullDateTimeFormat = "2006-01-02 15:04:05"
		dateOnlyFormat     = "2006-01-02"
	)

	for i, dc := range dateCounts {
		var t time.Time
		var err error

		t, err = time.Parse(fullDateTimeFormat, dc.Date)
		if err != nil {
			t, err = time.Parse(dateOnlyFormat, dc.Date)
			if err != nil {
				log.Printf("Error parsing date %s: %v", dc.Date, err)
				continue
			}
		}

		timestamp := float64(t.Unix())
		log.Printf("Parsed Row %d: original = %s, timestamp = %f, human readable = %s",
			i, dc.Date, timestamp, time.Unix(int64(timestamp), 0).Format("2006-01-02 15:04:05"))

		xValues = append(xValues, timestamp)
		yValues = append(yValues, float64(dc.Count))
	}

	gr := plot.NewDataDateForGraph(xValues, yValues, "Количество строковых полей", "Количество строк по времени", timeUnit)
	graphData, err := plot.DrawPlotBar(gr)

	if err != nil {
		log.Printf("Error generating time series plot: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка генерации графика")
		api.Send(msg)
		return
	}

	// Send statistics message
	statsMsg := fmt.Sprintf(
		"Статистика по датам в колонке %s:\n\n"+
			"• Всего записей: %d\n"+
			"• Уникальных дат: %d\n"+
			"• Период: с %s по %s\n",
		columnName,
		int(sum(yValues)),
		len(dateCounts),
		dateCounts[0].Date,
		dateCounts[len(dateCounts)-1].Date,
	)

	msg := tgbotapi.NewMessage(update.Message.Chat.ID, statsMsg)
	api.Send(msg)
	sumFirstColumnDate(db, dateTruncExpr, string(tableName), baseField, columnName, update, api, timeUnit)
	sendGraphVisualization(graphData, "timeseries", columnName, gr.GetNameGraph(), update.Message.Chat.ID, api, timeUnit)
}

// sum возвращает сумму всех значений в слайсе float64
func sum(values []float64) float64 {
	var total float64
	for _, v := range values {
		total += v
	}
	return total
}
func handleColumnDetails(api *tgbotapi.BotAPI, update tgbotapi.Update, columnName string) {
	tableName, exists := currentTable[update.Message.Chat.ID]
	if !exists {
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Сначала выберите таблицу")
		api.Send(msg)
		return
	}

	// Подключаемся к базе
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Printf("Error connecting to database: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка подключения к базе данных")
		api.Send(msg)
		return
	}
	statsMsg1, err := GenerateHistogramForString(db, tableName, columnName)
	if err != nil {
		log.Printf("Error generating plot: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка генерации графика")
		api.Send(msg)
		return
	}
	statsMsg2, err := analyzeNumericData(db, tableName)
	if err != nil {
		log.Printf("Error generating plot: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка генерации графика")
		api.Send(msg)
		return
	}
	sendGraphVisualization(statsMsg2, "FrequencyPlot", columnName, "", update.Message.Chat.ID, api)
	sendGraphVisualization(statsMsg1, "AggregationPlot", columnName, "", update.Message.Chat.ID, api)

}

func GenerateHistogramForString(db *gorm.DB, tableName models.ClickhouseTableName, columnName string) ([]byte, error) {
	// SQL для получения категориальных данных с подсчетом
	categoricalSQL := fmt.Sprintf(`
    SELECT %[1]s as category, 
           COUNT(*) as count
    FROM %[2]s
    WHERE %[1]s IS NOT NULL AND %[1]s != ''
    GROUP BY %[1]s
    ORDER BY count DESC
    LIMIT 20
`, columnName, tableName)

	type CategoryCount struct {
		Category string
		Count    int64
	}

	var categoryCounts []CategoryCount
	if err := db.Raw(categoricalSQL).Scan(&categoryCounts).Error; err != nil {
		return nil, fmt.Errorf("error getting category counts: %v", err)
	}

	// Подготовка данных для графика
	categories := make([]string, len(categoryCounts))
	counts := make([]float64, len(categoryCounts))
	maxCount := float64(0)

	for i, cc := range categoryCounts {
		categories[i] = cc.Category
		counts[i] = float64(cc.Count)
		if counts[i] > maxCount {
			maxCount = counts[i]
		}
	}
	hist := plot.NewDataXStringsForGraph(categories, counts, "", "", "")
	return plot.DrawPlotBar(hist)
}
func generateDetailsTextFieldColumn(db *gorm.DB, tableName models.ClickhouseTableName, columnName string) (string, error) {

	// SQL для получения основной статистики строковых значений
	statsSQL := fmt.Sprintf(`
        SELECT 
            count(*) as total_count,
            uniq(%[1]s) as unique_count,
            count(CASE WHEN %[1]s = '' THEN 1 END) as empty_count,
            count(CASE WHEN trim(%[1]s) = '' THEN 1 END) as whitespace_count,
            min(length(%[1]s)) as min_length,
            max(length(%[1]s)) as max_length,
            avg(length(%[1]s)) as avg_length
        FROM %[2]s
    `, columnName, tableName)

	var stats struct {
		TotalCount      int64
		UniqueCount     int64
		EmptyCount      int64
		WhitespaceCount int64
		MinLength       int
		MaxLength       int
		AvgLength       float64
	}

	if err := db.Raw(statsSQL).Scan(&stats).Error; err != nil {
		log.Printf("Error getting string statistics: %v", err)
		return "", fmt.Errorf("Error getting string statistics: %s", err)
	}

	// SQL для получения самых популярных и редких значений
	frequencySQL := fmt.Sprintf(`
        (SELECT 
            %[1]s as value,
            count(*) as count,
            'most_common' as type
        FROM %[2]s
        WHERE %[1]s != ''
        GROUP BY %[1]s
        ORDER BY count(*) DESC
        LIMIT 5)
        UNION ALL
        (SELECT 
            %[1]s as value,
            count(*) as count,
            'least_common' as type
        FROM %[2]s
        WHERE %[1]s != ''
        GROUP BY %[1]s
        ORDER BY count(*) ASC
        LIMIT 5)
    `, columnName, tableName)

	var frequencyResults []models.FrequencyData
	if err := db.Raw(frequencySQL).Scan(&frequencyResults).Error; err != nil {
		log.Printf("Error getting frequency data: %v", err)
		return "", fmt.Errorf("Error getting frequency data: %s", err)
	}

	// SQL для получения распределения длин строк
	lengthDistSQL := fmt.Sprintf(`
        SELECT 
            length(%[1]s) as str_length,
            count(*) as count
        FROM %[2]s
        WHERE %[1]s != ''
        GROUP BY length(%[1]s)
        ORDER BY str_length
        LIMIT 10
    `, columnName, tableName)

	var lengthDist []models.LengthDistribution
	if err := db.Raw(lengthDistSQL).Scan(&lengthDist).Error; err != nil {
		log.Printf("Error getting length distribution: %v", err)
		return "", fmt.Errorf("Error getting length distribution: %s", err)
	}

	// Формируем сообщение с результатами
	var mostCommon, leastCommon []string
	for _, fr := range frequencyResults {
		if fr.Type == "most_common" {
			mostCommon = append(mostCommon, fmt.Sprintf("'%s' (%d раз)", fr.Value, fr.Count))
		} else {
			leastCommon = append(leastCommon, fmt.Sprintf("'%s' (%d раз)", fr.Value, fr.Count))
		}
	}

	var lengthDistStr []string
	for _, ld := range lengthDist {
		lengthDistStr = append(lengthDistStr, fmt.Sprintf("длина %d: %d строк", ld.StrLength, ld.Count))
	}

	statsMsg := fmt.Sprintf(
		"Анализ строковых значений в колонке %s:\n\n"+
			"Общая статистика:\n"+
			"• Всего значений: %d\n"+
			"• Уникальных значений: %d (%.1f%%)\n\n"+
			"Пропуски и пустые строки:\n"+
			"• Пустых строк: %d (%.1f%%)\n"+
			"• Строк только с пробелами: %d (%.1f%%)\n\n"+
			"Длина строк:\n"+
			"• Минимальная длина: %d\n"+
			"• Максимальная длина: %d\n"+
			"• Средняя длина: %.1f\n\n"+
			"Топ-5 самых частых значений:\n%s\n\n"+
			"Топ-5 самых редких значений:\n%s\n\n"+
			"Распределение длин строк:\n%s",
		columnName,
		stats.TotalCount,
		stats.UniqueCount, float64(stats.UniqueCount)/float64(stats.TotalCount)*100,
		stats.EmptyCount, float64(stats.EmptyCount)/float64(stats.TotalCount)*100,
		stats.WhitespaceCount, float64(stats.WhitespaceCount)/float64(stats.TotalCount)*100,
		stats.MinLength,
		stats.MaxLength,
		stats.AvgLength,
		strings.Join(mostCommon, "\n"),
		strings.Join(leastCommon, "\n"),
		strings.Join(lengthDistStr, "\n"))
	return statsMsg, nil
}
func GenerateColumnHistogram(db *gorm.DB, tableName models.ClickhouseTableName, columnName string) (string, error) {
	// SQL для получения квантилей
	quantileSQL := fmt.Sprintf(`
        SELECT quantiles(0.01,0.05, 0.1, 0.5, 
                        0.9, 0.95, 0.99)(%s) as borders
        FROM %s
    `, columnName, tableName)

	// Структура для сканирования результата
	var result struct {
		Borders string
	}

	if err := db.Raw(quantileSQL).Scan(&result).Error; err != nil {
		return "", fmt.Errorf("error getting quantiles: %v", err)
	}

	// Парсим строку с массивом
	// Удаляем квадратные скобки и разбиваем по запятой
	bordersStr := strings.Trim(result.Borders, "[]")
	bordersSlice := strings.Split(bordersStr, ",")

	borders := make([]float64, len(bordersSlice))
	for i, s := range bordersSlice {
		// Парсим строку в float64
		val, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return "", fmt.Errorf("error parsing border value: %v", err)
		}
		borders[i] = val
	}

	// SQL для получения количества значений в каждом диапазоне
	var histData []models.HistogramData
	for i := 0; i < len(borders)-1; i++ {
		rangeSQL := fmt.Sprintf(`
            SELECT 
                %f as range_start,
                %f as range_end,
                count(*) as count
            FROM %s
            WHERE %s >= %f AND %s <= %f
        `, borders[i], borders[i+1], tableName, columnName, borders[i], columnName, borders[i+1])

		var rangeData models.HistogramData
		if err := db.Raw(rangeSQL).Scan(&rangeData).Error; err != nil {
			return "", fmt.Errorf("error getting range data: %v", err)
		}
		histData = append(histData, rangeData)
	}
	return "", nil
	// return generateSVGHistogram(histData, columnName), nil
}

// func generateSVGHistogram(histData []models.HistogramData, columnName string) string {
// 	// Параметры SVG
// 	width := 800
// 	height := 400
// 	padding := 70 // Увеличили padding для меток осей
// 	graphWidth := width - 2*padding
// 	graphHeight := height - 2*padding

// 	// Находим максимальные значения для осей
// 	maxCount := 0
// 	maxValue := histData[len(histData)-1].RangeEnd
// 	minValue := histData[0].RangeStart

// 	for _, data := range histData {
// 		if data.Count > maxCount {
// 			maxCount = data.Count
// 		}
// 	}

// 	// Создаем точки для кривой Безье
// 	points := make([]string, len(histData))
// 	for i, data := range histData {
// 		// Используем логарифмическую шкалу для Y
// 		yLog := math.Log1p(float64(data.Count))
// 		yMaxLog := math.Log1p(float64(maxCount))

// 		x := padding + int(float64(i)*float64(graphWidth)/float64(len(histData)-1))
// 		y := height - padding - int((yLog/yMaxLog)*float64(graphHeight))

// 		if i == 0 {
// 			points[i] = fmt.Sprintf("M %d %d", x, y)
// 		} else {
// 			// Создаем контрольные точки для кривой Безье
// 			prevX := padding + int(float64(i-1)*float64(graphWidth)/float64(len(histData)-1))
// 			ctrlX1 := prevX + (x-prevX)/2
// 			ctrlX2 := x - (x-prevX)/2
// 			points[i] = fmt.Sprintf("C %d %d %d %d %d %d",
// 				ctrlX1, y, ctrlX2, y, x, y)
// 		}
// 	}

// 	// Начало SVG с определением градиента и стилей
// 	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d">
//         <defs>
//             <linearGradient id="gradient" x1="0%%" y1="0%%" x2="0%%" y2="100%%">
//                 <stop offset="0%%" style="stop-color:#4682B4;stop-opacity:0.8"/>
//                 <stop offset="100%%" style="stop-color:#4682B4;stop-opacity:0.2"/>
//             </linearGradient>
//         </defs>
//         <style>
//             .axis { stroke: #333; stroke-width: 1; }
//             .axis-label { font-family: Arial; font-size: 12px; fill: #666; }
//             .title { font-family: Arial; font-size: 16px; fill: #333; }
//             .grid { stroke: #ddd; stroke-width: 0.5; }
//             .curve { fill: none; stroke: #4682B4; stroke-width: 2; }
//             .area { fill: url(#gradient); opacity: 0.5; }
//             .tick-label { font-family: Arial; font-size: 10px; fill: #666; }
//         </style>`, width, height)

// 	// Добавляем заголовок
// 	svg += fmt.Sprintf(`
//         <text x="%d" y="30" text-anchor="middle" class="title">
//             Распределение значений: %s (логарифмическая шкала)
//         </text>`, width/2, columnName)

// 	// Добавляем сетку и метки для оси Y
// 	yTicks := 5
// 	for i := 0; i <= yTicks; i++ {
// 		y := height - padding - int(float64(i)*float64(graphHeight)/float64(yTicks))
// 		value := math.Exp(float64(i)*math.Log1p(float64(maxCount))/float64(yTicks)) - 1

// 		// Горизонтальная линия сетки
// 		svg += fmt.Sprintf(`
//             <line x1="%d" y1="%d" x2="%d" y2="%d" class="grid"/>
//             <text x="%d" y="%d" text-anchor="end" class="tick-label">%d</text>`,
// 			padding, y, width-padding, y,
// 			padding-5, y+4, int(value))
// 	}

// 	// Добавляем метки для оси X
// 	xTicks := 5
// 	for i := 0; i <= xTicks; i++ {
// 		x := padding + int(float64(i)*float64(graphWidth)/float64(xTicks))
// 		value := minValue + (maxValue-minValue)*float64(i)/float64(xTicks)

// 		// Вертикальная линия сетки
// 		svg += fmt.Sprintf(`
//             <line x1="%d" y1="%d" x2="%d" y2="%d" class="grid"/>
//             <text x="%d" y="%d" text-anchor="middle" class="tick-label" transform="rotate(-45 %d,%d)">%.1f</text>`,
// 			x, padding, x, height-padding,
// 			x, height-padding+20, x, height-padding+20, value)
// 	}

// 	// Добавляем оси
// 	svg += fmt.Sprintf(`
//         <line x1="%d" y1="%d" x2="%d" y2="%d" class="axis"/>
//         <line x1="%d" y1="%d" x2="%d" y2="%d" class="axis"/>`,
// 		padding, padding, padding, height-padding, // Y axis
// 		padding, height-padding, width-padding, height-padding) // X axis

// 	// Добавляем подписи осей
// 	svg += fmt.Sprintf(`
//         <text x="%d" y="%d" transform="rotate(-90 %d,%d)"
//               text-anchor="middle" class="axis-label">Количество (log scale)</text>
//         <text x="%d" y="%d" text-anchor="middle" class="axis-label">Значение</text>`,
// 		padding-40, height/2, padding-40, height/2,
// 		width/2, height-5)

// 	// Добавляем кривую и область под ней
// 	pathD := strings.Join(points, " ")
// 	areaPath := pathD + fmt.Sprintf(" L %d %d L %d %d Z",
// 		width-padding, height-padding,
// 		padding, height-padding)

// 	svg += fmt.Sprintf(`
//         <path d="%s" class="area"/>
//         <path d="%s" class="curve"/>`,
// 		areaPath, pathD)

// 	svg += "</svg>"
// 	return svg
// }

func GenerateHistogram(db *gorm.DB, tableName models.ClickhouseTableName, columnName string) ([]byte, error, []byte) {
	// SQL запрос для получения гистограммы
	histogramSQL := fmt.Sprintf(`
        WITH 
            min_max AS (
                SELECT min(%[1]s) as min_val, max(%[1]s) as max_val 
                FROM %[2]s 
                WHERE %[1]s IS NOT NULL
            ),
            hist AS (
                SELECT 
                    arrayJoin(histogram(20)(%[1]s)) as hist_row
                FROM %[2]s
            )
        SELECT 
            hist_row.1 as range_start,
            hist_row.2 as range_end,
            hist_row.3 as count
        FROM hist
        ORDER BY range_start;
    `, columnName, tableName)

	// Определяем структуру с корректными тегами
	type HistogramData struct {
		RangeStart float64 `db:"range_start"`
		RangeEnd   float64 `db:"range_end"`
		Count      float64 `db:"count"`
	}

	var histData []HistogramData
	if err := db.Raw(histogramSQL).Scan(&histData).Error; err != nil {
		return nil, fmt.Errorf("error getting histogram data: %v", err), nil
	}
	xStart := make([]float64, len(histData))
	xEnd := make([]float64, len(histData))
	// Логируем полученные данные
	log.Printf("Received %d data points", len(histData))
	for i, data := range histData {
		log.Printf("Data point %d: Start=%f, End=%f, Count=%f",
			i, data.RangeStart, data.RangeEnd, data.Count)
	}

	// Подготавливаем данные для визуализации
	xValues := make([]float64, len(histData))
	yValues := make([]float64, len(histData))

	for i, data := range histData {
		// Используем середину диапазона для оси X
		xStart[i] = data.RangeStart
		xEnd[i] = data.RangeEnd
		yValues[i] = data.Count
		xValues[i] = (xStart[i] + xEnd[i]) / 2
		log.Printf("Point %d: XStart=%f,XEnd:=%f ,Y=%f", i, xStart[i], xEnd[i], yValues[i])
	}

	histBar := plot.NewDataRangeXValuesForGraph(xStart, xEnd, yValues, "", "", "")
	// Генерируем гистограмму
	hist, _ := plot.DrawPlotBar(histBar)
	graph, _ := plot.DrawDensityPlot(xValues, yValues)
	return hist, nil, graph
}
func handleGraphColumn(api *tgbotapi.BotAPI, update tgbotapi.Update, columnName string) {
	tableName, exists := currentTable[update.Message.Chat.ID]
	if !exists {
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Сначала выберите таблицу")
		api.Send(msg)
		return
	}
	// Подключаемся к базе
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Printf("Error connecting to database: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка подключения к базе данных")
		api.Send(msg)
		return
	}

	// SQL для получения основной статистики и квантилей
	statsSQL := fmt.Sprintf(`
	SELECT 
	min(%[1]s) as min_value,
	max(%[1]s) as max_value,
	avg(%[1]s) as avg_value,
	median(%[1]s) as median_value,
	count(%[1]s) as total_count,
	quantile(0.25)(%[1]s) as q1,
	quantile(0.75)(%[1]s) as q3,
	quantiles(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)(%[1]s) as main_quantiles
	FROM %[2]s
    `, columnName, tableName)

	var stats struct {
		MinValue      float64
		MaxValue      float64
		AvgValue      float64
		MedianValue   float64
		TotalCount    int64
		Q1            float64
		Q3            float64
		MainQuantiles string
	}

	if err := db.Raw(statsSQL).Scan(&stats).Error; err != nil {
		log.Printf("Error getting statistics: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка получения статистики")
		api.Send(msg)
		return
	}

	// Рассчитываем IQR и границы выбросов
	iqr := stats.Q3 - stats.Q1
	lowerBound := stats.Q1 - 1.5*iqr
	upperBound := stats.Q3 + 1.5*iqr

	// SQL для подсчета выбросов
	outlierSQL := fmt.Sprintf(`
        SELECT 
            count(*) as outliers_count,
            min(%[1]s) as min_outlier,
            max(%[1]s) as max_outlier
        FROM %[2]s
        WHERE %[1]s < %[3]f OR %[1]s > %[4]f
    `, columnName, tableName, lowerBound, upperBound)

	var outlierStats struct {
		OutliersCount int64
		MinOutlier    float64
		MaxOutlier    float64
	}

	if err := db.Raw(outlierSQL).Scan(&outlierStats).Error; err != nil {
		log.Printf("Error getting outlier statistics: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка получения статистики по выбросам")
		api.Send(msg)
		return
	}

	// SQL для получения квантилей для графика (10 точек)
	quantilesSQL := fmt.Sprintf(`
        SELECT quantiles(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(%s) as quantiles
        FROM %s
    `, columnName, tableName)

	var result struct {
		Quantiles string
	}

	if err := db.Raw(quantilesSQL).Scan(&result).Error; err != nil {
		log.Printf("Error getting quantiles: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка получения квантилей")
		api.Send(msg)
		return
	}

	// Парсим квантили для графика
	bordersStr := strings.Trim(result.Quantiles, "[]")
	bordersSlice := strings.Split(bordersStr, ",")

	// Подготавливаем массивы для графика
	xValues := make([]float64, len(bordersSlice))
	yValues := make([]float64, len(bordersSlice))

	// Для каждого диапазона считаем количество значений
	for i := 0; i < len(bordersSlice); i++ {
		borderValue, err := strconv.ParseFloat(strings.TrimSpace(bordersSlice[i]), 64)
		if err != nil {
			log.Printf("Error parsing border value: %v", err)
			continue
		}

		var prevBorder float64
		if i == 0 {
			prevBorder = stats.MinValue
		} else {
			prevBorder, _ = strconv.ParseFloat(strings.TrimSpace(bordersSlice[i-1]), 64)
		}

		// SQL для подсчета значений в диапазоне
		rangeSQL := fmt.Sprintf(`
            SELECT COUNT(*) as count
            FROM %s
            WHERE %s >= %f AND %s < %f
        `, tableName, columnName, prevBorder, columnName, borderValue)

		var rangeCount struct {
			Count int64
		}

		if err := db.Raw(rangeSQL).Scan(&rangeCount).Error; err != nil {
			log.Printf("Error getting range count: %v", err)
			continue
		}

		xValues[i] = (prevBorder + borderValue) / 2
		yValues[i] = float64(rangeCount.Count)
	}

	pngData, err, pngData2 := GenerateHistogram(db, tableName, columnName)
	if err != nil {
		log.Printf("Error generating plot: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка генерации графика")
		api.Send(msg)
		return
	}

	// Парсим основные квантили для вывода
	mainQuantilesStr := strings.Trim(stats.MainQuantiles, "[]")
	mainQuantilesSlice := strings.Split(mainQuantilesStr, ",")
	quantileValues := make([]float64, len(mainQuantilesSlice))
	for i, q := range mainQuantilesSlice {
		quantileValues[i], _ = strconv.ParseFloat(strings.TrimSpace(q), 64)
	}

	// Создаем подробное сообщение со статистикой
	statsMsg := fmt.Sprintf(
		"Статистика по колонке %s:\n\n"+
			"Основные показатели:\n"+
			"• Минимальное значение: %.2f\n"+
			"• Максимальное значение: %.2f\n"+
			"• Среднее значение: %.2f\n"+
			"• Медиана: %.2f\n"+
			"• Всего значений: %d\n\n"+
			"Квантили:\n"+
			"• 1%% (P01): %.2f\n"+
			"• 5%% (P05): %.2f\n"+
			"• 10%% (P10): %.2f\n"+
			"• 25%% (Q1): %.2f\n"+
			"• 50%% (Median): %.2f\n"+
			"• 75%% (Q3): %.2f\n"+
			"• 90%% (P90): %.2f\n"+
			"• 95%% (P95): %.2f\n"+
			"• 99%% (P99): %.2f\n\n"+
			"Анализ выбросов:\n"+
			"• Количество выбросов: %d\n"+
			"• Диапазон выбросов: %.2f - %.2f",
		columnName,
		stats.MinValue, stats.MaxValue, stats.AvgValue, stats.MedianValue, stats.TotalCount,
		quantileValues[0], quantileValues[1], quantileValues[2],
		quantileValues[3], quantileValues[4], quantileValues[5],
		quantileValues[6], quantileValues[7], quantileValues[8],
		outlierStats.OutliersCount,
		outlierStats.MinOutlier, outlierStats.MaxOutlier)

	msg := tgbotapi.NewMessage(update.Message.Chat.ID, statsMsg)
	api.Send(msg)

	// Отправляем график
	sendGraphVisualization(pngData, "histogram", columnName, "частотное распределения категориальных данных", update.Message.Chat.ID, api)
	sendGraphVisualization(pngData2, "density", columnName, "суммирование числовых данных по категориям", update.Message.Chat.ID, api)

}

func analyzeStringColumn(db *gorm.DB, tableName string, columnName string) (*models.StringColumnStats, error) {
	stats := &models.StringColumnStats{}

	// Основная статистика
	basicStatsSQL := fmt.Sprintf(`
        SELECT 
            COUNT(*) as total_rows,
            uniq(%[1]s) as unique_values,
            COUNT(*) - COUNT(%[1]s) as null_count,
            SUM(CASE WHEN %[1]s = '' THEN 1 ELSE 0 END) as empty_string_count,
            SUM(CASE WHEN length(trim(%[1]s)) = 0 THEN 1 ELSE 0 END) as whitespace_count,
            min(length(%[1]s)) as min_length,
            max(length(%[1]s)) as max_length,
            avg(length(%[1]s)) as avg_length
        FROM %[2]s
    `, columnName, tableName)

	var basicStats models.BasicStats
	if err := db.Raw(basicStatsSQL).Scan(&basicStats).Error; err != nil {
		return nil, fmt.Errorf("error getting basic stats: %v", err)
	}

	stats.TotalRows = basicStats.TotalRows
	stats.UniqueValues = basicStats.UniqueValues
	stats.NullCount = basicStats.NullCount
	stats.EmptyStringCount = basicStats.EmptyStringCount
	stats.WhitespaceCount = basicStats.WhitespaceCount
	stats.MinLength = basicStats.MinLength
	stats.MaxLength = basicStats.MaxLength
	stats.AvgLength = basicStats.AvgLength

	// Популярные значения
	popularSQL := fmt.Sprintf(`
        WITH value_counts AS (
            SELECT 
                %s as value,
                COUNT(*) as count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
            FROM %s
            WHERE %s IS NOT NULL
            GROUP BY %s
        )
        SELECT value, count, percentage
        FROM value_counts
        ORDER BY count DESC
        LIMIT 10
    `, columnName, tableName, columnName, columnName)

	if err := db.Raw(popularSQL).Scan(&stats.PopularValues).Error; err != nil {
		return nil, fmt.Errorf("error getting popular values: %v", err)
	}

	// Непопулярные значения
	unpopularSQL := fmt.Sprintf(`
        WITH value_counts AS (
            SELECT 
                %s as value,
                COUNT(*) as count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
            FROM %s
            WHERE %s IS NOT NULL
            GROUP BY %s
        )
        SELECT value, count, percentage
        FROM value_counts
        ORDER BY count ASC
        LIMIT 10
    `, columnName, tableName, columnName, columnName)

	if err := db.Raw(unpopularSQL).Scan(&stats.UnpopularValues).Error; err != nil {
		return nil, fmt.Errorf("error getting unpopular values: %v", err)
	}

	// Распределение длин строк
	lengthDistSQL := fmt.Sprintf(`
        WITH lengths AS (
            SELECT length(%s) as str_length
            FROM %s
            WHERE %s IS NOT NULL
        )
        SELECT 
            str_length as length,
            COUNT(*) as frequency
        FROM lengths
        GROUP BY str_length
        ORDER BY str_length
    `, columnName, tableName, columnName)

	if err := db.Raw(lengthDistSQL).Scan(&stats.LengthDistribution).Error; err != nil {
		return nil, fmt.Errorf("error getting length distribution: %v", err)
	}

	return stats, nil
}

func handleStartCommand(api *tgbotapi.BotAPI, update tgbotapi.Update) {
	message := update.Message

	f, err := os.ReadFile("welcome.xml")

	if err != nil {
		log.Printf("[Error] open welcome file: %v", err)
	}
	msg := tgbotapi.NewMessage(message.Chat.ID, string(f))
	api.Send(msg)
	return

}

func analyzeNumericData(db *gorm.DB, tableName models.ClickhouseTableName) ([]byte, error) {
	// Получаем информацию о структуре таблицы, исключая первую колонку (обычно id)
	tableInfoSQL := fmt.Sprintf(`
        SELECT 
            name,
            type,
            position
        FROM system.columns 
        WHERE table = '%s'
        AND position > 1  -- пропускаем первую колонку (обычно id)
        AND (type LIKE '%%Int%%' OR type LIKE '%%Float%%')
        ORDER BY position
        LIMIT 1
    `, tableName)

	type ColumnInfo struct {
		Name     string
		Type     string
		Position int
	}

	var numericColumn ColumnInfo
	if err := db.Raw(tableInfoSQL).Scan(&numericColumn).Error; err != nil {
		return nil, fmt.Errorf("error finding numeric column: %v", err)
	}

	if numericColumn.Name == "" {
		return nil, fmt.Errorf("no numeric columns found")
	}

	log.Printf("Found numeric column: %s (%s) at position %d",
		numericColumn.Name, numericColumn.Type, numericColumn.Position)

	// Находим первое строковое поле для группировки (исключая поле с датой)
	findGroupColumnSQL := fmt.Sprintf(`
        SELECT 
            name,
            type,
            position
        FROM system.columns 
        WHERE table = '%s'
        AND type LIKE '%%String%%'
        AND position > 1
        AND name NOT LIKE '%%date%%'
        ORDER BY position
        LIMIT 1
    `, tableName)

	var groupColumn ColumnInfo
	if err := db.Raw(findGroupColumnSQL).Scan(&groupColumn).Error; err != nil {
		return nil, fmt.Errorf("error finding group column: %v", err)
	}

	if groupColumn.Name == "" {
		return nil, fmt.Errorf("no string columns found for grouping")
	}

	log.Printf("Using group column: %s (%s)", groupColumn.Name, groupColumn.Type)

	// Выполняем агрегацию
	aggregateSQL := fmt.Sprintf(`
        SELECT 
            %s as category,
            sum(%s) as total
        FROM %s
        WHERE %s IS NOT NULL
        GROUP BY %s
        ORDER BY total DESC
    `, groupColumn.Name, numericColumn.Name, tableName, groupColumn.Name, groupColumn.Name)

	type Result struct {
		Category string
		Total    int64
	}

	var results []Result
	if err := db.Raw(aggregateSQL).Scan(&results).Error; err != nil {
		return nil, fmt.Errorf("error aggregating data: %v", err)
	}

	log.Printf("Found %d aggregated results", len(results))

	if len(results) == 0 {
		return nil, fmt.Errorf("no data found after aggregation")
	}

	// Готовим данные для графика
	categories := make([]string, len(results))
	values := make([]float64, len(results))
	for i, r := range results {
		categories[i] = r.Category
		values[i] = float64(r.Total)
		log.Printf("Result %d: %s = %d", i, r.Category, r.Total)
	}

	// Генерируем график
	data := plot.NewDataXStringsForGraph(categories, values, "Histogram String Value", "Frequency", "")
	return plot.DrawPlotBar(data)
}

func sumFirstColumnDate(db *gorm.DB, dateTruncExpr, tableName, baseField, columnName string, update tgbotapi.Update, api *tgbotapi.BotAPI, timeUnit string) {
	// Get first numeric column
	var numericColumn string
	numericColSQL := fmt.Sprintf(`
	SELECT name
	FROM system.columns 
	WHERE table = '%s' 
	AND type IN ('Int8', 'Int16', 'Int32', 'Int64', 'Float32', 'Float64', 'Decimal') 
	LIMIT 1`, tableName)
	err := db.Raw(numericColSQL).Scan(&numericColumn).Error
	if err != nil {
		log.Printf("Error getting numeric column: %v", err)
	}
	// Prepare SQL query with optional numeric aggregation
	var dateSQL string
	if numericColumn != "" {
		dateSQL = fmt.Sprintf(`
			SELECT 
				toString(%s) as date,
				count(*) as count,
				sum(%s) as sum_value
			FROM %s 
			WHERE %s IS NOT NULL 
			GROUP BY %s 
			ORDER BY %s
		`, dateTruncExpr, numericColumn, tableName, baseField, dateTruncExpr, dateTruncExpr)
	}

	var dateCounts []models.DateCount
	if err := db.Raw(dateSQL).Scan(&dateCounts).Error; err != nil {
		log.Printf("Error getting date counts: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка получения статистики по датам")
		api.Send(msg)
		return
	}

	// Добавляем отладочный вывод
	log.Printf("Raw SQL query: %s", dateSQL)
	log.Printf("Number of results: %d", len(dateCounts))
	for i, dc := range dateCounts {
		log.Printf("Row %d: date = %s, count = %d", i, dc.Date, dc.Count)
	}
	// Также проверим значения после парсинга
	xValues := make([]float64, 0, len(dateCounts))
	yValues := make([]float64, 0, len(dateCounts))

	const (
		fullDateTimeFormat = "2006-01-02 15:04:05"
		dateOnlyFormat     = "2006-01-02"
	)

	for i, dc := range dateCounts {
		var t time.Time
		var err error

		t, err = time.Parse(fullDateTimeFormat, dc.Date)
		if err != nil {
			t, err = time.Parse(dateOnlyFormat, dc.Date)
			if err != nil {
				log.Printf("Error parsing date %s: %v", dc.Date, err)
				continue
			}
		}

		timestamp := float64(t.Unix())
		log.Printf("Parsed Row %d: original = %s, timestamp = %f, human readable = %s",
			i, dc.Date, timestamp, time.Unix(int64(timestamp), 0).Format("2006-01-02 15:04:05"))

		xValues = append(xValues, timestamp)
		yValues = append(yValues, float64(dc.SumValue))
	}
	// созадем структуру для реализации функции
	gr := plot.NewDataDateForGraph(xValues, yValues, columnName, fmt.Sprintf("Суммарное значение столбца %s группировка по времени", numericColumn), timeUnit)
	graphData, err := plot.DrawPlotBar(gr)

	if err != nil {
		log.Printf("Error generating time series plot: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка генерации графика")
		api.Send(msg)
		return
	}

	// Send statistics message

	sendGraphVisualization(graphData, "timeseries", columnName, gr.GetNameGraph(), update.Message.Chat.ID, api)
}
