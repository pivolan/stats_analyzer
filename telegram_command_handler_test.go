package main

import (
	"fmt"
	"github.com/pivolan/stats_analyzer/config"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"gorm.io/gorm/logger"
	"testing"
)

func TestGenerateColumnHistogram(t *testing.T) {
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	assert.NoError(t, err)

	_, err = GenerateColumnHistogram(db, "0001_timestamp_0002_value_0db8e6", "0002_value")
	assert.NoError(t, err)
}
func TestGenerateDetailsTextFieldColumn(t *testing.T) {
	cfg := config.GetConfig()
	db, err := gorm.Open(mysql.Open(cfg.DatabaseDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	assert.NoError(t, err)

	info, err := generateDetailsTextFieldColumn(db, "0001_id_0002_managerid_0003_splittestid_c5a6c8", "0039_executedos")
	assert.NoError(t, err)
	fmt.Println(info)
}
