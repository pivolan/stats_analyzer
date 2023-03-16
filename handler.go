package main

import (
	"github.com/go-telegram-bot-api/telegram-bot-api"
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
	unpackedFilePath, err := unpackArchive(filePath)
	if err != nil {
		log.Printf("Error unpacking file: %v", err)
		return
	}
	if unpackedFilePath != "" {
		filePath = unpackedFilePath
	}

	// Import data into ClickHouse
	tableName, err := importDataIntoClickHouse(filePath)
	if err != nil {
		log.Printf("Error importing data into ClickHouse: %v", err)
		return
	}

	// Analyze data and send result to user
	go analyzeStatistics(bot, message, tableName)
}

func analyzeStatistics(bot *tgbotapi.BotAPI, message *tgbotapi.Message, tableName string) {
	// TODO: Implement analyzing statistics and sending result to user
}
