package main

import (
	"github.com/go-telegram-bot-api/telegram-bot-api"
	"log"
)

func main() {
	bot, err := tgbotapi.NewBotAPI("6232707025:AAECU6gOFwNwug-I7tjrWPq9ML6kOFBiru8")
	if err != nil {
		log.Fatal(err)
	}

	bot.Debug = true

	log.Printf("Authorized on account %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

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
