package main

import (
	"fmt"
	"github.com/go-telegram-bot-api/telegram-bot-api"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

func handleText(bot *tgbotapi.BotAPI, update tgbotapi.Update) {
	helpText := `This bot is for statistics analyzer. You can upload a CSV file here in any format, and we will analyze this data and show you summaries. The file can be any data size, and it can be gzip, lz4, or zip archived.`

	msg := tgbotapi.NewMessage(update.Message.Chat.ID, helpText)
	bot.Send(msg)
}

func handleDocument(bot *tgbotapi.BotAPI, message *tgbotapi.Message) {
	// Save document to disk
	fileURL, err := bot.GetFileDirectURL(message.Document.FileID)
	if err != nil {
		log.Printf("Error getting file URL: %v", err)
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
	defer file.Close()
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		log.Printf("Error writing file: %v", err)
		return
	}

	// Unpack archive if necessary
	handleFile(filePath)
}
func handleFile(filePath string) string {
	// Unpack archive if necessary
	unpackedFilePath, err := unpackArchive(filePath)
	if err != nil {
		log.Printf("Error unpacking file: %v", err)
		return ""
	}
	if unpackedFilePath != "" {
		filePath = unpackedFilePath
	}

	// Import data into ClickHouse
	tableName, err := importDataIntoClickHouse(filePath)
	if err != nil {
		log.Printf("Error importing data into ClickHouse: %v", err)
		return ""
	}
	fmt.Print(tableName)
	analyzeStatistics(tableName)
	return tableName
}
func analyzeStatistics(tableName string) {
	dsn := "default:@tcp(127.0.0.1:9004)/default"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		log.Fatalln("cannot connect to clickhouse", err)
	}

	columnsInfo, err := getColumnAndTypeList(db, tableName)
	fmt.Println(columnsInfo, err)
	queries := generateQueries(tableName, columnsInfo)
	fmt.Println(queries)
	for i, query := range queries {
		result := map[string]interface{}{}
		db.Raw(query.Sql).Scan(&result)
		fmt.Println(i, "query:", query)
		for key, value := range result {
			fmt.Println(key, value)
		}
	}
}
