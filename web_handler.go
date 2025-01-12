package main

import (
	"fmt"
	"github.com/go-telegram-bot-api/telegram-bot-api"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

func handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get the file from the form data
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "Error uploading file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Get the uuid from the form data
	uuid := r.FormValue("uuid")
	if uuid == "" {
		http.Error(w, "Error getting uuid", http.StatusBadRequest)
		return
	}

	// Save the uploaded file to a local folder with the uuid as the filename
	os.MkdirAll(filepath.Join("uploads", uuid), 0755)
	filePath := filepath.Join("uploads", uuid, header.Filename)
	dst, err := os.Create(filePath)
	if err != nil {
		http.Error(w, "Error create saving file"+err.Error(), http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	_, err = io.Copy(dst, file)
	if err != nil {
		http.Error(w, "Error saving file", http.StatusInternalServerError)
		return
	}
	if chatId, ok := users[uuid]; ok {
		msg := tgbotapi.NewMessage(chatId, "Your file uploaded, wait for first results")
		bot.Send(msg)
	}

	go func(uuid string, filePath string) {
		stat := handleFile(filePath)
		if chatId, ok := users[uuid]; ok {
			sendStats(chatId, stat, bot)
		}
	}(uuid, filePath)

	fmt.Fprintf(w, "File uploaded successfully")
}
