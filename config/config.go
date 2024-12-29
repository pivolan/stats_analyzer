package config

import (
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseDSN   string
	TelegramToken string
}

var (
	config *Config
	once   sync.Once
)

// GetConfig возвращает singleton экземпляр конфигурации
func GetConfig() *Config {
	once.Do(func() {
		err := godotenv.Load()
		if err != nil {
			log.Fatal("Error loading .env file")
		}

		config = &Config{
			DatabaseDSN:   os.Getenv("DATABASE_DSN"),
			TelegramToken: os.Getenv("TELEGRAM_TOKEN"),
		}
	})
	return config
}
