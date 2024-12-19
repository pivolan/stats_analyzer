package config

import (
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
)

type Config struct {
	DbDsn   string
	TgToken string
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
			DbDsn:   os.Getenv("DB_DSN"),
			TgToken: os.Getenv("TG_TOKEN"),
		}
	})
	return config
}
