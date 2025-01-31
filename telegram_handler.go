package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pivolan/stats_analyzer/config"
	"github.com/pivolan/stats_analyzer/domain/models"
	"github.com/pivolan/stats_analyzer/plot"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	uuid "github.com/satori/go.uuid"
)

// telegram_handler.go

var currentTable = map[int64]models.ClickhouseTableName{}
var toDelete = map[string]time.Time{}

var toDeleteTable = map[models.ClickhouseTableName]time.Time{}

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
		Select(stat, chatId, bot)
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

func Select(stat map[string]CommonStat, chatID int64, bot *tgbotapi.BotAPI) {
	const (
		fullDateTimeFormat = "2006-01-02 15:04:05"
		dateOnlyFormat     = "2006-01-02"
		maxPoints          = 20
	)

	// Порядок интервалов от меньшего к большему
	intervals := []string{"hour", "day", "month", "year"}

	// Для каждого интервала пытаемся построить график
	for _, interval := range intervals {
		x := []float64{}
		y := []float64{}
		z := []float64{}

		// Ищем данные для текущего интервала
		for k, v := range stat {
			if strings.HasPrefix(k, "graph_") && strings.HasSuffix(k, interval) {
				if len(v.Dates) == 0 {
					continue
				}

				fmt.Println("GETALL", v)
				// Собираем все точки для данного интервала
				for _, point := range v.Dates {
					d := point["date"].(string)

					t, err := time.Parse(fullDateTimeFormat, d)
					if err != nil {
						t, err = time.Parse(dateOnlyFormat, d)
						if err != nil {
							log.Printf("Error parsing date %s: %v", d, err)
							continue
						}
					}

					timeStamp := float64(t.Unix())
					x = append(x, timeStamp)
					z = append(z, float64(point["cnt"].(int64)))
					y = append(y, float64(point["sum_value"].(int64)))
				}

				// Если точек больше maxPoints, переходим к следующему интервалу
				if len(x) > maxPoints {
					log.Printf("Too many points (%d) for interval %s, trying next interval",
						len(x), interval)
					break
				}

				// Если у нас есть подходящее количество точек, строим графики
				if len(x) > 0 && len(x) <= maxPoints {
					objectGraph := plot.NewDataDateForGraph(
						x,
						z,
						"count",
						fmt.Sprintf("Показывает количество строк по времени в таблице", k[6:]),
						interval,
					)
					graph, err := plot.DrawPlotBar(objectGraph)
					if err == nil {
						sendGraphVisualization(graph, "timeseries", "count",
							objectGraph.GetNameGraph(), chatID, bot)
					}

					objectGraph1 := plot.NewDataDateForGraph(
						x,
						y,
						"sum",
						fmt.Sprintf("суммарное значение столбца %v по времени", v.Title),
						interval,
					)
					graph1, err := plot.DrawPlotBar(objectGraph1)
					if err == nil {
						sendGraphVisualization(graph1, "timeseries", "sum",
							objectGraph1.GetNameGraph(), chatID, bot)
					}

					// Графики построены успешно, выходим из функции
					return
				}
			}
		}
	}

	// Если не удалось построить графики ни для одного интервала
	log.Printf("Could not create graphs for any time interval")
}
