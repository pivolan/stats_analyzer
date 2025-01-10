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

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	uuid "github.com/satori/go.uuid"
)

var toDelete = map[string]time.Time{}

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
	welcomeText := `Привет! 👋

Я помогу проанализировать ваши данные и создать статистические отчёты. 

Что я умею:
- Анализирую CSV файлы любого размера
- Анализирую последовательности чисел (просто отправьте числа в чат)
- Поддерживаю архивы (gzip, lz4, zip)
- Создаю подробную статистику по всем колонкам
- Генерирую графики распределения данных
- Строю временные ряды и агрегации

Как со мной работать:
1. Отправьте CSV файл прямо в чат
2. Или отправьте последовательность чисел для анализа
3. Или напишите любое сообщение для получения ссылки на веб-загрузку

Примеры отправки чисел:
- "1 2 3 4 5"
- "1,2,3,4,5"
- "1\n2\n3\n4\n5"
`
	if message != nil && message.IsCommand() {
		args := strings.TrimSpace(update.Message.CommandArguments())
		parts := strings.Split(args, " ")
		if len(parts) == 1 && parts[0] == "" {
			parts = []string{}
		}
	}
	switch update.Message.Command() {
	case "start":
		msg := tgbotapi.NewMessage(message.Chat.ID, welcomeText)

		_, err := bot.Send(msg)
		if err != nil {
			return
		}
	}

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
	go func() {
		stat := handleFile(filePath)
		sendStats(message.Chat.ID, stat, bot)
		//files with dates
	}()
}

func sendStats(chatId int64, stat map[string]CommonStat, bot *tgbotapi.BotAPI) {
	// Отправляем основную статистику
	formattedText := GenerateTable(stat)
	formattedTexts := GenerateGroupsTables(stat)

	msg := tgbotapi.NewMessage(chatId, "<pre>\n"+formattedText+"\n</pre>")
	msg.ParseMode = tgbotapi.ModeHTML
	_, err := bot.Send(msg)
	if err != nil {
		return
	}

	// Отправляем файлы с общей информацией
	data := tgbotapi.FileBytes{
		Name:  "stats" + time.Now().Format("20060102-150405") + ".txt",
		Bytes: []byte(formattedText),
	}
	msg2 := tgbotapi.NewDocumentUpload(chatId, data)
	msg2.Caption = "Общая статистика"
	_, err = bot.Send(msg2)
	if err != nil {
		return
	}

	// Отправляем статистику по группам
	if len(formattedTexts) > 0 {
		data = tgbotapi.FileBytes{
			Name:  "stats_groups" + time.Now().Format("20060102-150405") + ".txt",
			Bytes: []byte(strings.Join(formattedTexts, "\n")),
		}
		msg2 = tgbotapi.NewDocumentUpload(chatId, data)
		msg2.Caption = "Статистика по группам"
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
			zipFile := tgbotapi.FileBytes{
				Name:  "time_series_data_" + time.Now().Format("20060102-150405") + ".zip",
				Bytes: zipData,
			}
			zipMsg := tgbotapi.NewDocumentUpload(chatId, zipFile)
			zipMsg.Caption = "CSV файлы с группировкой по времени"
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
			svg1Msg.Caption = "График 1: Временной ряд"
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
			svg2Msg.Caption = "График 2: Временной ряд"
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
func handleFile(filePath string) map[string]CommonStat {
	// Unpack archive if necessary
	unpackedFilePath, err := unpackArchive(filePath)
	if err != nil {
		log.Printf("Error unpacking file: %v", err)
		return nil
	}
	if unpackedFilePath != "" {
		filePath = unpackedFilePath
	}

	// Import data into ClickHouse
	tableName, err := importDataIntoClickHouse(filePath)
	if err != nil {
		log.Printf("Error importing data into ClickHouse: %v", err)
		return nil
	}
	fmt.Print(tableName)
	return analyzeStatistics(tableName)
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
	width, height := 800.0, 400.0
	padding := struct{ top, right, bottom, left float64 }{50.0, 60.0, 70.0, 60.0}
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
