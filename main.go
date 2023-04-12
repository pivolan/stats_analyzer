package main

import (
	"fmt"
	"github.com/go-telegram-bot-api/telegram-bot-api"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"html/template"
	"log"
	"net/http"
	"os"
	"time"
)

var users = map[string]int64{}
var bot *tgbotapi.BotAPI

func main() {
	dsn := "default:@tcp(127.0.0.1:9004)/default"
	_, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		log.Fatalln("cannot connect to clickhouse", err)
	}

	bot, err = tgbotapi.NewBotAPI("6232707025:AAECU6gOFwNwug-I7tjrWPq9ML6kOFBiru8")
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Get the uuid from the URL
		id := r.URL.Query().Get("id")

		// Generate the upload form page with the uuid field pre-filled
		tmpl := template.Must(template.ParseFiles("upload.html"))
		err := tmpl.Execute(w, id)
		if err != nil {
			http.Error(w, "Error rendering upload form", http.StatusInternalServerError)
			return
		}
	})

	// Handle a POST request to /upload with a file upload form
	http.HandleFunc("/upload", handleUpload)

	bot.Debug = true

	log.Printf("Authorized on account %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	go func() {
		fmt.Println("listen on: http://localhost:8005")
		err := http.ListenAndServe(":8005", nil)
		if err != nil {
			fmt.Println("Error starting server:", err)
			os.Exit(1)
		}
	}()
	updates, err := bot.GetUpdatesChan(u)
	go func() {
		for {
			time.Sleep(time.Minute)
			for uid, date := range toDelete {
				if time.Now().After(date.Add(time.Hour * 1)) {
					delete(users, uid)
				}
			}
		}
	}()
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
