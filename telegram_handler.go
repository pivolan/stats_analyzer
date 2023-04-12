package main

import (
	"fmt"
	"github.com/go-telegram-bot-api/telegram-bot-api"
	uuid "github.com/satori/go.uuid"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var toDelete = map[string]time.Time{}

func handleText(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	helpText := `This bot is for statistics analyzer. You can upload a CSV file here in any format, and we will analyze this data and show you summaries. The file can be any data size, and it can be gzip, lz4, or zip archived.`
	uid := uuid.NewV4()
	message := update.Message
	users[uid.String()] = message.Chat.ID
	msg := tgbotapi.NewMessage(message.Chat.ID, "upload by this link: https://statsdata.org/?id="+uid.String())
	toDelete[uid.String()] = time.Now()
	bot.Send(msg)
	msg = tgbotapi.NewMessage(update.Message.Chat.ID, helpText)
	bot.Send(msg)
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
		formattedText := GenerateTable(stat)
		fmt.Println(formattedText)
		msg := tgbotapi.NewMessage(message.Chat.ID, "<pre>\n"+formattedText+"\n</pre>")
		msg.ParseMode = tgbotapi.ModeHTML
		bot.Send(msg)
		data := tgbotapi.FileBytes{Name: "stats" + time.Now().Format("20060102-150405") + ".txt", Bytes: []byte(formattedText)}
		msg2 := tgbotapi.NewDocumentUpload(message.Chat.ID, data)
		msg2.Caption = "file"
		bot.Send(msg2)

	}()
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
func analyzeStatistics(tableName string) map[string]CommonStat {
	dsn := "default:default@tcp(127.0.0.1:9004)/default"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		log.Fatalln("cannot connect to clickhouse", err)
	}

	columnsInfo, err := getColumnAndTypeList(db, tableName)
	fmt.Println(columnsInfo, err)
	sql1 := generateSqlForNumericColumnsStats(columnsInfo, tableName)
	fmt.Println(sql1)
	sql2 := generateSqlForUniqCounts(columnsInfo, tableName)
	fmt.Println(sql2)
	numericInfo := map[string]interface{}{}
	tx := db.Raw(sql1)
	tx.Scan(numericInfo)
	fmt.Println(numericInfo)
	uniqInfo := map[string]interface{}{}
	tx2 := db.Raw(sql2)
	tx2.Scan(uniqInfo)
	fmt.Println(uniqInfo)
	r1 := parseUniqResults(uniqInfo)
	r2 := parseNumericResults(numericInfo)
	r := mergeStat(r1, r2)
	return r
}
