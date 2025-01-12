package main

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/pivolan/stats_analyzer/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DfgjertGoddmERhU59 click
// 7975342417:AAHusII9MKeJb9OeVscUnZnvMnVwC7wDE0U tg

// this variable is used for assoc between upload file link and message.chat.id telegram, to send stats in chat
var users = map[string]int64{}
var bot *tgbotapi.BotAPI

func main() {
	cfg := config.GetConfig()
	fmt.Println("started")
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		log.Fatalln("cannot connect to clickhouse", err)
	}
	go func() {
		for {
			<-time.Tick(time.Second)
			for table, timer := range toDeleteTable {
				if time.Now().After(timer) {
					db.Exec(fmt.Sprintf(`drop table "%s"`, string(table)))
					delete(toDeleteTable, table)
					log.Println("dropped table", table)
				}
			}
		}
	}()
	fmt.Println("connected clickhouse")

	bot, err = tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("bot init")

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
		fmt.Println("start listen http 8005")
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
			removeOldFiles("upload", time.Now().Add(-time.Hour*2))
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
	fmt.Println("started all")
}

func removeOldFiles(dirPath string, maxAge time.Time) error {
	// Get a list of files and directories in the directory
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	// Loop through each file/directory
	for _, file := range files {
		filePath := filepath.Join(dirPath, file.Name())

		// If the file is a directory, recursively call this function on it
		if file.IsDir() {
			err := removeOldFiles(filePath, maxAge)
			if err != nil {
				return err
			}
		} else {
			// If the file is older than the max age, remove it
			fileStat, err := os.Stat(filePath)
			if err != nil {
				return err
			}
			fileModTime := fileStat.ModTime()
			if fileModTime.Before(maxAge) {
				err := os.Remove(filePath)
				if err != nil {
					return err
				}
				fmt.Printf("Removed file: %s\n", filePath)
			}
		}
	}

	return nil
}
