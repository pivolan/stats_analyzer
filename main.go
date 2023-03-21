package main

import (
	"fmt"
	"github.com/go-telegram-bot-api/telegram-bot-api"
	uuid2 "github.com/satori/go.uuid"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func main() {
	bot, err := tgbotapi.NewBotAPI("6232707025:AAECU6gOFwNwug-I7tjrWPq9ML6kOFBiru8")
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Get the uuid from the URL
		uuid, _ := uuid2.NewV4()

		// Generate the upload form page with the uuid field pre-filled
		tmpl := template.Must(template.ParseFiles("upload.html"))
		err := tmpl.Execute(w, uuid.String())
		if err != nil {
			http.Error(w, "Error rendering upload form", http.StatusInternalServerError)
			return
		}
	})

	// Handle a POST request to /upload with a file upload form
	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
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

		handleFile(filePath)

		fmt.Fprintf(w, "File uploaded successfully")
	})

	bot.Debug = true

	log.Printf("Authorized on account %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	go func() {
		err := http.ListenAndServe(":8005", nil)
		if err != nil {
			fmt.Println("Error starting server:", err)
			os.Exit(1)
		}
	}()
	updates, err := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil {
			continue
		}

		if update.Message.Document != nil {
			go handleDocument(bot, update.Message)
		} else if update.Message.Text != "" {
			go handleText(bot, update)
		}
	}
}
