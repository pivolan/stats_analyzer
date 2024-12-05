package main

import (
	"fmt"
	"github.com/go-telegram-bot-api/telegram-bot-api"
	uuid "github.com/satori/go.uuid"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var toDelete = map[string]time.Time{}

func handleText(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	welcomeText := `Привет! 👋

Я помогу проанализировать ваши данные и создать статистические отчёты. 

Что я умею:
- Анализирую CSV файлы любого размера
- Поддерживаю архивы (gzip, lz4, zip)
- Создаю подробную статистику по всем колонкам
- Генерирую графики распределения данных
- Строю временные ряды и агрегации

Как со мной работать:
1. Отправьте CSV файл прямо в чат
2. Или напишите любое сообщение, чтобы получить персональную ссылку для веб-загрузки файла

После обработки вы получите:
- Текстовый отчет с основной статистикой
- Файл с детальным анализом групп данных
- CSV файлы с временными рядами (если есть даты)

Для начала просто отправьте мне файл с данными!

`
	//helpText := `This bot is for statistics analyzer. You can upload a CSV file here in any format, and we will analyze this data and show you summaries. The file can be any data size, and it can be gzip, lz4, or zip archived.`
	uid := uuid.NewV4()
	message := update.Message
	users[uid.String()] = message.Chat.ID

	if message.IsCommand() {
		switch message.Command() {
		case "start":
			msg := tgbotapi.NewMessage(message.Chat.ID, welcomeText)
			_, err := bot.Send(msg)
			if err != nil {
				return
			}

			time.Sleep(time.Second * 5)
			msg = tgbotapi.NewMessage(message.Chat.ID, "Перейдите по ссылке чтобы загрузить файл: https://statsdata.org/?id="+uid.String())
			toDelete[uid.String()] = time.Now()
			_, err = bot.Send(msg)
			if err != nil {
				return
			}
			return
		}
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
		bot.Send(msg)
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
	formattedText := GenerateTable(stat)
	formattedTexts := GenerateGroupsTables(stat)
	csvFiles := GenerateCSVByDates(stat)

	msg := tgbotapi.NewMessage(chatId, "<pre>\n"+formattedText+"\n</pre>")
	msg.ParseMode = tgbotapi.ModeHTML
	bot.Send(msg)
	//files with common infos
	data := tgbotapi.FileBytes{Name: "stats" + time.Now().Format("20060102-150405") + ".txt", Bytes: []byte(formattedText)}
	msg2 := tgbotapi.NewDocumentUpload(chatId, data)
	msg2.Caption = "file"
	bot.Send(msg2)
	data = tgbotapi.FileBytes{Name: "stats_groups" + time.Now().Format("20060102-150405") + ".txt", Bytes: []byte(strings.Join(formattedTexts, "\n"))}
	msg2 = tgbotapi.NewDocumentUpload(chatId, data)
	msg2.Caption = "file"
	bot.Send(msg2)
	if len(csvFiles) > 2 {
		b := ZipArchive(csvFiles)
		data = tgbotapi.FileBytes{Name: "stats_dates" + time.Now().Format("20060102-150405") + ".zip", Bytes: b}
		msg2 = tgbotapi.NewDocumentUpload(chatId, data)
		msg2.Caption = "use https://www.csvplot.com/ for great presentation of this data, use datetime column for date axis, common column for legend, and cnt as main count"
		bot.Send(msg2)
	} else {
		for _, csvData := range csvFiles {
			data = tgbotapi.FileBytes{Name: "stats_dates" + time.Now().Format("20060102-150405") + ".csv", Bytes: []byte(csvData)}
			msg2 = tgbotapi.NewDocumentUpload(chatId, data)
			msg2.Caption = "file"
			bot.Send(msg2)
		}
	}
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
