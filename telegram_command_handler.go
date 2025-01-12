package main

import (
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/pivolan/stats_analyzer/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"math"
	"strconv"
	"strings"
	"time"
)

// HistogramData представляет данные для одного столбца гистограммы
type HistogramData struct {
	RangeStart float64
	RangeEnd   float64
	Count      int
}

func handleCommand(api *tgbotapi.BotAPI, update tgbotapi.Update) {
	// Получаем полную команду (без слеша)
	fullCommand := update.Message.Command()

	// Префиксы команд
	graphPrefix := "graph_"
	detailsPrefix := "details_"

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

	default:
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Неизвестная команда. Используйте: /graph<имя_колонки> или /details<имя_колонки>")
		api.Send(msg)
	}
}

func handleColumnDetails(api *tgbotapi.BotAPI, update tgbotapi.Update, columnName string) {
	// Создаем сообщение с деталями колонки
	msg := tgbotapi.NewMessage(update.Message.Chat.ID,
		fmt.Sprintf("Статистика по колонке: %s", columnName))

	// TODO: Здесь будет логика получения статистики по колонке
	// Например, мин/макс значения, среднее, медиана, распределение и т.д.

	api.Send(msg)
}

func GenerateColumnHistogram(db *gorm.DB, tableName ClickhouseTableName, columnName string) (string, error) {
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
	var histData []HistogramData
	for i := 0; i < len(borders)-1; i++ {
		rangeSQL := fmt.Sprintf(`
            SELECT 
                %f as range_start,
                %f as range_end,
                count(*) as count
            FROM %s
            WHERE %s >= %f AND %s <= %f
        `, borders[i], borders[i+1], tableName, columnName, borders[i], columnName, borders[i+1])

		var rangeData HistogramData
		if err := db.Raw(rangeSQL).Scan(&rangeData).Error; err != nil {
			return "", fmt.Errorf("error getting range data: %v", err)
		}
		histData = append(histData, rangeData)
	}
	return generateSVGHistogram(histData, columnName), nil
}
func generateSVGHistogram(histData []HistogramData, columnName string) string {
	// Параметры SVG
	width := 800
	height := 400
	padding := 70 // Увеличили padding для меток осей
	graphWidth := width - 2*padding
	graphHeight := height - 2*padding

	// Находим максимальные значения для осей
	maxCount := 0
	maxValue := histData[len(histData)-1].RangeEnd
	minValue := histData[0].RangeStart

	for _, data := range histData {
		if data.Count > maxCount {
			maxCount = data.Count
		}
	}

	// Создаем точки для кривой Безье
	points := make([]string, len(histData))
	for i, data := range histData {
		// Используем логарифмическую шкалу для Y
		yLog := math.Log1p(float64(data.Count))
		yMaxLog := math.Log1p(float64(maxCount))

		x := padding + int(float64(i)*float64(graphWidth)/float64(len(histData)-1))
		y := height - padding - int((yLog/yMaxLog)*float64(graphHeight))

		if i == 0 {
			points[i] = fmt.Sprintf("M %d %d", x, y)
		} else {
			// Создаем контрольные точки для кривой Безье
			prevX := padding + int(float64(i-1)*float64(graphWidth)/float64(len(histData)-1))
			ctrlX1 := prevX + (x-prevX)/2
			ctrlX2 := x - (x-prevX)/2
			points[i] = fmt.Sprintf("C %d %d %d %d %d %d",
				ctrlX1, y, ctrlX2, y, x, y)
		}
	}

	// Начало SVG с определением градиента и стилей
	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d">
        <defs>
            <linearGradient id="gradient" x1="0%%" y1="0%%" x2="0%%" y2="100%%">
                <stop offset="0%%" style="stop-color:#4682B4;stop-opacity:0.8"/>
                <stop offset="100%%" style="stop-color:#4682B4;stop-opacity:0.2"/>
            </linearGradient>
        </defs>
        <style>
            .axis { stroke: #333; stroke-width: 1; }
            .axis-label { font-family: Arial; font-size: 12px; fill: #666; }
            .title { font-family: Arial; font-size: 16px; fill: #333; }
            .grid { stroke: #ddd; stroke-width: 0.5; }
            .curve { fill: none; stroke: #4682B4; stroke-width: 2; }
            .area { fill: url(#gradient); opacity: 0.5; }
            .tick-label { font-family: Arial; font-size: 10px; fill: #666; }
        </style>`, width, height)

	// Добавляем заголовок
	svg += fmt.Sprintf(`
        <text x="%d" y="30" text-anchor="middle" class="title">
            Распределение значений: %s (логарифмическая шкала)
        </text>`, width/2, columnName)

	// Добавляем сетку и метки для оси Y
	yTicks := 5
	for i := 0; i <= yTicks; i++ {
		y := height - padding - int(float64(i)*float64(graphHeight)/float64(yTicks))
		value := math.Exp(float64(i)*math.Log1p(float64(maxCount))/float64(yTicks)) - 1

		// Горизонтальная линия сетки
		svg += fmt.Sprintf(`
            <line x1="%d" y1="%d" x2="%d" y2="%d" class="grid"/>
            <text x="%d" y="%d" text-anchor="end" class="tick-label">%d</text>`,
			padding, y, width-padding, y,
			padding-5, y+4, int(value))
	}

	// Добавляем метки для оси X
	xTicks := 5
	for i := 0; i <= xTicks; i++ {
		x := padding + int(float64(i)*float64(graphWidth)/float64(xTicks))
		value := minValue + (maxValue-minValue)*float64(i)/float64(xTicks)

		// Вертикальная линия сетки
		svg += fmt.Sprintf(`
            <line x1="%d" y1="%d" x2="%d" y2="%d" class="grid"/>
            <text x="%d" y="%d" text-anchor="middle" class="tick-label" transform="rotate(-45 %d,%d)">%.1f</text>`,
			x, padding, x, height-padding,
			x, height-padding+20, x, height-padding+20, value)
	}

	// Добавляем оси
	svg += fmt.Sprintf(`
        <line x1="%d" y1="%d" x2="%d" y2="%d" class="axis"/>
        <line x1="%d" y1="%d" x2="%d" y2="%d" class="axis"/>`,
		padding, padding, padding, height-padding, // Y axis
		padding, height-padding, width-padding, height-padding) // X axis

	// Добавляем подписи осей
	svg += fmt.Sprintf(`
        <text x="%d" y="%d" transform="rotate(-90 %d,%d)" 
              text-anchor="middle" class="axis-label">Количество (log scale)</text>
        <text x="%d" y="%d" text-anchor="middle" class="axis-label">Значение</text>`,
		padding-40, height/2, padding-40, height/2,
		width/2, height-5)

	// Добавляем кривую и область под ней
	pathD := strings.Join(points, " ")
	areaPath := pathD + fmt.Sprintf(" L %d %d L %d %d Z",
		width-padding, height-padding,
		padding, height-padding)

	svg += fmt.Sprintf(`
        <path d="%s" class="area"/>
        <path d="%s" class="curve"/>`,
		areaPath, pathD)

	svg += "</svg>"
	return svg
}

// Пример использования в обработчике команды
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

	// Генерируем гистограмму
	svg, err := GenerateColumnHistogram(db, tableName, columnName)
	if err != nil {
		log.Printf("Error generating histogram: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка генерации графика"+err.Error())
		api.Send(msg)
		return
	}
	fileName := fmt.Sprintf("histogram_%s_%s.svg",
		columnName,
		time.Now().Format("20060102-150405"))

	// Создаем файл для отправки
	svgFile := tgbotapi.FileBytes{
		Name:  fileName,
		Bytes: []byte(svg),
	}

	// Создаем сообщение с файлом
	docMsg := tgbotapi.NewDocumentUpload(update.Message.Chat.ID, svgFile)
	docMsg.Caption = fmt.Sprintf("Гистограмма распределения: %s", fileName)

	// Отправляем файл
	_, err = api.Send(docMsg)
	if err != nil {
		log.Printf("Error sending SVG: %v", err)
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка отправки графика")
		api.Send(msg)
		return
	}

	// Отправляем описание графика
	description := fmt.Sprintf("График показывает распределение значений в колонке %s\n"+
		"Данные разбиты на диапазоны по квантилям для лучшей визуализации\n"+
		"Высота столбцов показывает количество значений в каждом диапазоне",
		columnName)

	descMsg := tgbotapi.NewMessage(update.Message.Chat.ID, description)
	api.Send(descMsg)
}
