package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pivolan/stats_analyzer/config"
	"github.com/pivolan/stats_analyzer/domain/models"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	uuid "github.com/satori/go.uuid"
)

var toDelete = map[string]time.Time{}
var currentTable = map[int64]models.ClickhouseTableName{}
var toDeleteTable = map[models.ClickhouseTableName]time.Time{}

// telegram_handler.go

func handleText(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	message := update.Message
	text := message.Text

	// Проверяем, есть ли числа в сообщении
	numbers := ExtractNumbers(text)
	if len(numbers) > 0 {
		// Анализируем числа
		stats := AnalyzeNumbers(numbers)
		formattedStats := FormatStats(stats)

		// Отправляем результат
		msg := tgbotapi.NewMessage(message.Chat.ID, formattedStats)
		_, err := bot.Send(msg)
		if err != nil {
			return
		}
		return
	}

	// Если чисел нет, продолжаем стандартную обработку

	uid := uuid.NewV4()
	users[uid.String()] = message.Chat.ID
	msg := tgbotapi.NewMessage(message.Chat.ID, "Перейдите по ссылке чтобы загрузить файл: https://statsdata.org/?id="+uid.String())
	toDelete[uid.String()] = time.Now()
	_, err := bot.Send(msg)
	if err != nil {
		return
	}
}

func handleDocument(bot *tgbotapi.BotAPI, message *tgbotapi.Message) {
	// Save document to disk
	fileURL, err := bot.GetFileDirectURL(message.Document.FileID)
	if err != nil {
		log.Printf("Error getting file URL: %v", err)
		uid := uuid.NewV4()
		users[uid.String()] = message.Chat.ID
		msg := tgbotapi.NewMessage(message.Chat.ID, "Error on upload file, if file too big try another method, upload by this link: https://statsdata.org/?id="+uid.String())
		_, err := bot.Send(msg)
		if err != nil {
			return
		}
		return
	}

	// Download file to disk
	filePath := filepath.Join(".", strconv.Itoa(message.From.ID), message.Document.FileName)
	err = os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
	if err != nil {
		log.Printf("Error creating directory: %v", err)
		return
	}
	resp, err := http.Get(fileURL)
	if err != nil {
		log.Printf("Error downloading file: %v", err)
		return
	}
	defer resp.Body.Close()
	file, err := os.Create(filePath)
	if err != nil {
		log.Printf("Error creating file: %v", err)
		return
	}
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		log.Printf("Error writing file: %v", err)
		return
	}

	// Unpack archive if necessary
	go func(filePath string, chatId int64) {
		table, err := handleFile(filePath)
		if err != nil {
			msg := tgbotapi.NewMessage(chatId, "Some error on processing file:"+err.Error())
			bot.Send(msg)
			return
		}
		fmt.Println("import finished", table)
		currentTable[chatId] = table
		stat := analyzeStatistics(table)
		fmt.Println("analyze finished", stat)
		sendStats(chatId, stat, bot)
		toDeleteTable[table] = time.Now().Add(time.Hour)
		//files with dates
	}(filePath, message.Chat.ID)
}

// splitMessage splits a long message into parts at logical breaks
func splitMessage(text string, maxLength int) []string {
	if len(text) <= maxLength {
		return []string{text}
	}

	var messages []string
	remainingText := text

	for len(remainingText) > 0 {
		// Calculate the actual split index based on remaining text length
		splitIndex := maxLength
		if len(remainingText) < maxLength {
			splitIndex = len(remainingText)
		}

		searchStart := splitIndex - 1000
		if searchStart < 0 {
			searchStart = 0
		}

		// Search for natural break points in the valid range
		searchText := remainingText[:splitIndex]
		lastDoubleBreak := strings.LastIndex(searchText[searchStart:], "\n\n")
		if lastDoubleBreak != -1 {
			splitIndex = searchStart + lastDoubleBreak
		} else {
			lastSingleBreak := strings.LastIndex(searchText[searchStart:], "\n")
			if lastSingleBreak != -1 {
				splitIndex = searchStart + lastSingleBreak
			}
		}

		// Extract the part and append to messages
		part := remainingText[:splitIndex]
		messages = append(messages, strings.TrimSpace(part))

		// Update remaining text
		if splitIndex >= len(remainingText) {
			break
		}
		remainingText = strings.TrimSpace(remainingText[splitIndex:])
	}

	return messages
}

func sendStats(chatId int64, stat map[string]CommonStat, bot *tgbotapi.BotAPI) {
	formattedText := GenerateCommonInfoMsg(stat)

	// Split long messages
	const maxLength = 4000
	messages := splitMessage(formattedText, maxLength)
	fmt.Println(messages)
	// Send each part of the main message
	for _, msgText := range messages {
		msg := tgbotapi.NewMessage(chatId, msgText)
		_, err := bot.Send(msg)
		if err != nil {
			log.Printf("Error sending message: %v", err)
			return
		}
	}

	// Отправляем файлы с общей информацией
	fileName := "stats" + time.Now().Format("20060102-150405") + ".txt"
	data := tgbotapi.FileBytes{
		Name:  fileName,
		Bytes: []byte(fileName + "\n" + formattedText),
	}
	msg2 := tgbotapi.NewDocumentUpload(chatId, data)
	msg2.Caption = "Общая статистика: " + fileName
	_, err := bot.Send(msg2)
	if err != nil {
		return
	}

	// Отправляем статистику по группам
	formattedTexts := GenerateGroupsTables(stat)
	if len(formattedTexts) > 0 {
		fileName := "stats_groups" + time.Now().Format("20060102-150405") + ".txt"
		data = tgbotapi.FileBytes{
			Name:  fileName,
			Bytes: []byte(fileName + "\n" + strings.Join(formattedTexts, "\n")),
		}
		msg2 = tgbotapi.NewDocumentUpload(chatId, data)
		msg2.Caption = "Статистика по группам: " + fileName
		_, err = bot.Send(msg2)
		if err != nil {
			return
		}
	}

	// Анализируем временные ряды
	timeSeriesData := analyzeTimeSeriesFiles(stat)
	if len(timeSeriesData) > 0 {
		graph1Data, graph2Data := selectGraphData(timeSeriesData)

		// Генерируем CSV файлы для временных рядов
		csvFiles := GenerateCSVByDates(stat)
		zipData := ZipArchive(csvFiles)
		if zipData != nil {
			fileName := "time_series_data_" + time.Now().Format("20060102-150405") + ".zip"
			zipFile := tgbotapi.FileBytes{
				Name:  fileName,
				Bytes: zipData,
			}
			zipMsg := tgbotapi.NewDocumentUpload(chatId, zipFile)
			zipMsg.Caption = "CSV файлы с группировкой по времени: " + fileName
			_, err = bot.Send(zipMsg)
			if err != nil {
				return
			}
		}

		if graph1Data != nil {
			// Генерируем первый график
			svg1 := generateTimeSeriesSVG(graph1Data.Data,
				fmt.Sprintf("Time Series - %s Grouping", graph1Data.Period))

			// Сохраняем SVG файл
			svg1FileName := fmt.Sprintf("timeseries_1_%s_%s.svg",
				graph1Data.Period, time.Now().Format("20060102-150405"))
			svg1File := tgbotapi.FileBytes{
				Name:  svg1FileName,
				Bytes: []byte(svg1),
			}
			svg1Msg := tgbotapi.NewDocumentUpload(chatId, svg1File)
			svg1Msg.Caption = "График 1: Временной ряд: " + svg1FileName
			_, err = bot.Send(svg1Msg)
			if err != nil {
				return
			}

			// Отправляем описание первого графика
			descMsg1 := tgbotapi.NewMessage(chatId,
				generateTimeSeriesDescription(graph1Data, 1))
			_, err = bot.Send(descMsg1)
			if err != nil {
				return
			}
		}

		if graph2Data != nil && graph2Data != graph1Data {
			// Генерируем второй график
			svg2 := generateTimeSeriesSVG(graph2Data.Data,
				fmt.Sprintf("Time Series - %s Grouping", graph2Data.Period))

			// Сохраняем SVG файл
			svg2FileName := fmt.Sprintf("timeseries_2_%s_%s.svg",
				graph2Data.Period, time.Now().Format("20060102-150405"))
			svg2File := tgbotapi.FileBytes{
				Name:  svg2FileName,
				Bytes: []byte(svg2),
			}
			svg2Msg := tgbotapi.NewDocumentUpload(chatId, svg2File)
			svg2Msg.Caption = "График 2: Временной ряд: " + svg2FileName
			_, err = bot.Send(svg2Msg)
			if err != nil {
				return
			}

			// Отправляем описание второго графика
			descMsg2 := tgbotapi.NewMessage(chatId,
				generateTimeSeriesDescription(graph2Data, 2))
			_, err = bot.Send(descMsg2)
			if err != nil {
				return
			}
		}
	}
}

// WriteArtifact создает артефакт с указанным ID и содержимым
func WriteArtifact(id string, artifactType string, content string) error {
	artifactPath := fmt.Sprintf("artifacts/%s", id)
	err := os.MkdirAll(filepath.Dir(artifactPath), 0755)
	if err != nil {
		return fmt.Errorf("error creating artifact directory: %v", err)
	}

	err = os.WriteFile(artifactPath, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("error writing artifact file: %v", err)
	}

	return nil
}

func marshalJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "[]"
	}
	return string(b)
}
func handleFile(filePath string) (models.ClickhouseTableName, error) {
	// Unpack archive if necessary
	unpackedFilePath, err := unpackArchive(filePath)
	if err != nil {
		log.Printf("Error unpacking file: %v", err)
		return "", fmt.Errorf("handleFile>unpackArchive err: %s", err)
	}
	if unpackedFilePath != "" {
		filePath = unpackedFilePath
	}

	// Import data into ClickHouse
	// Подключаемся к базе данных
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		log.Println("error on connect to clickhouse", err)
		return "", fmt.Errorf("handleFile>gorm.Open err: %s", err)
	}

	tableName, err := importDataIntoClickHouse(filePath, db)
	if err != nil {
		log.Printf("Error importing data into ClickHouse: %v", err)
		return "", fmt.Errorf("handleFile>importDataIntoClickHouse err: %s", err)
	}
	fmt.Print(tableName)
	return tableName, nil
}

func generateTimeSeriesSVG(data []map[string]interface{}, title string) string {
	// Находим все числовые метрики
	metrics := []string{}
	if len(data) > 0 {
		for key, value := range data[0] {
			if key != "datetime" && key != "common" {
				if _, ok := value.(float64); ok {
					metrics = append(metrics, key)
				}
			}
		}
	}

	// Находим минимальные и максимальные значения
	minVal, maxVal := math.MaxFloat64, -math.MaxFloat64
	for _, item := range data {
		for _, metric := range metrics {
			if val, ok := item[metric].(float64); ok {
				if val < minVal {
					minVal = val
				}
				if val > maxVal {
					maxVal = val
				}
			}
		}
	}

	// Параметры графика
	width, height := 1000.0, 400.0
	padding := struct{ top, right, bottom, left float64 }{50.0, 260.0, 70.0, 60.0}
	chartWidth := width - padding.left - padding.right
	chartHeight := height - padding.top - padding.bottom

	// Цвета для линий
	colors := []string{"#8884d8", "#82ca9d", "#ffc658", "#ff7300", "#ff0000"}

	// Начинаем генерировать SVG
	svg := fmt.Sprintf(`<svg viewBox="0 0 %.0f %.0f" xmlns="http://www.w3.org/2000/svg">
    <defs>
        <style>
            text { font-family: Arial, sans-serif; font-size: 12px; }
            .title { font-size: 16px; font-weight: bold; }
            .axis-label { font-size: 10px; }
            .legend-text { font-size: 12px; }
        </style>
    </defs>`, width, height)

	// Добавляем заголовок
	svg += fmt.Sprintf(`<text x="%.0f" y="30" text-anchor="middle" class="title">%s</text>`,
		width/2, title)

	// Добавляем область графика
	svg += fmt.Sprintf(`<rect x="%.0f" y="%.0f" width="%.0f" height="%.0f" fill="none" stroke="#ccc" stroke-width="1"/>`,
		padding.left, padding.top, chartWidth, chartHeight)

	// Сетка
	for i := 0; i <= 5; i++ {
		y := padding.top + (chartHeight * float64(i) / 5.0)
		svg += fmt.Sprintf(`<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" stroke="#eee"/>`,
			padding.left, y, width-padding.right, y)
	}

	// Метки значений
	for i := 0; i <= 5; i++ {
		y := padding.top + (chartHeight * float64(i) / 5.0)
		value := maxVal - ((maxVal - minVal) * float64(i) / 5.0)
		svg += fmt.Sprintf(`<text x="%.0f" y="%.0f" text-anchor="end" class="axis-label">%.1f</text>`,
			padding.left-5, y+4, value)
	}

	// Данные
	for i, metric := range metrics {
		color := colors[i%len(colors)]
		points := make([]string, len(data))
		for j, item := range data {
			x := padding.left + (float64(j) * chartWidth / float64(len(data)-1))
			if val, ok := item[metric].(float64); ok {
				y := padding.top + chartHeight - ((val - minVal) * chartHeight / (maxVal - minVal))
				if j == 0 {
					points[j] = fmt.Sprintf("M%.1f,%.1f", x, y)
				} else {
					points[j] = fmt.Sprintf("L%.1f,%.1f", x, y)
				}
			}
		}
		svg += fmt.Sprintf(`<path d="%s" stroke="%s" fill="none" stroke-width="2"/>`,
			strings.Join(points, " "), color)
	}

	// Оси
	svg += fmt.Sprintf(`<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" stroke="#000"/>`,
		padding.left, height-padding.bottom, width-padding.right, height-padding.bottom)
	svg += fmt.Sprintf(`<line x1="%.0f" y1="%.0f" x2="%.0f" y2="%.0f" stroke="#000"/>`,
		padding.left, padding.top, padding.left, height-padding.bottom)

	// Метки времени
	for i, item := range data {
		if i%3 == 0 { // Показываем каждую третью метку для избежания перекрытия
			x := padding.left + (float64(i) * chartWidth / float64(len(data)-1))
			svg += fmt.Sprintf(`<text x="%.0f" y="%.0f" transform="rotate(-45 %.0f,%.0f)" 
                text-anchor="end" class="axis-label">%v</text>`,
				x, height-padding.bottom+20, x, height-padding.bottom+20,
				item["datetime"])
		}
	}

	// Легенда
	legendX := width - padding.right + 10
	for i, metric := range metrics {
		y := padding.top + float64(i)*20
		color := colors[i%len(colors)]
		svg += fmt.Sprintf(`
            <rect x="%.0f" y="%.0f" width="15" height="15" fill="%s"/>
            <text x="%.0f" y="%.0f" class="legend-text">%s</text>`,
			legendX, y, color, legendX+20, y+12, metric)
	}

	svg += "</svg>"
	return svg
}
