package main

import (
	"fmt"

	"github.com/pivolan/stats_analyzer/config"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"testing"

	"gorm.io/gorm/logger"
)

func TestGenerateColumnHistogram(t *testing.T) {
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	assert.NoError(t, err)

	_, err = GenerateColumnHistogram(db, "0001_price_0002_rating_0003_views_6d36fd", "0002_rating")
	assert.NoError(t, err)
}
func TestGenerateDetailsTextFieldColumn(t *testing.T) {
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	assert.NoError(t, err)

	info, err := generateDetailsTextFieldColumn(db, "0001_price_0002_rating_0003_views_6d36fd", "0039_executedos")
	assert.NoError(t, err)
	fmt.Println(info)
}
