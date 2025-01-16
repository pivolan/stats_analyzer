package main

import (
	"fmt"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"log"
	"time"
)

// sendGraphVisualization отправляет визуализацию в чат с соответствующими пояснениями.
// Параметры:
//   - graph: байтовый массив с данными изображения
//   - visualType: тип визуализации (histogram, timeseries, density)
//   - columnName: имя анализируемой колонки
//   - chatID: ID чата для отправки
//   - api: экземпляр Telegram API для отправки сообщений
//   - timeUnit: единица измерения времени (опционально, для временных рядов)
func sendGraphVisualization(graph []byte, visualType string, columnName string, chatID int64, api *tgbotapi.BotAPI, timeUnit ...string) {
	// Формируем имя файла с учетом типа визуализации и временной метки
	fileName := fmt.Sprintf("%s_%s_%s.png",
		visualType,
		columnName,
		time.Now().Format("20060102-150405"))

	// Подготавливаем файл для отправки
	pngFile := tgbotapi.FileBytes{
		Name:  fileName,
		Bytes: graph,
	}

	// Создаем сообщение с изображением
	docMsg := tgbotapi.NewPhotoUpload(chatID, pngFile)

	// Формируем поясняющий текст в зависимости от типа визуализации
	var caption string
	switch visualType {
	case "histogram":
		caption = fmt.Sprintf("Гистограмма распределения значений: %s\n"+
			"Показывает частоту встречаемости различных значений в данных.",
			columnName)
	case "density":
		caption = fmt.Sprintf("График плотности распределения: %s\n"+
			"Отображает непрерывное распределение вероятностей значений.",
			columnName)
	case "timeseries":
		timeUnitStr := ""
		if len(timeUnit) > 0 {
			timeUnitStr = fmt.Sprintf(" (группировка по %s)", timeUnit[0])
		}
		caption = fmt.Sprintf("Временной ряд: %s%s\n"+
			"Показывает изменение значений во времени.",
			columnName, timeUnitStr)
	default:
		caption = fmt.Sprintf("Визуализация данных: %s", columnName)
	}

	docMsg.Caption = caption

	// Отправляем сообщение с изображением
	_, err := api.Send(docMsg)
	if err != nil {
		log.Printf("Ошибка отправки визуализации %s для колонки %s: %v",
			visualType, columnName, err)
		errMsg := tgbotapi.NewMessage(chatID,
			fmt.Sprintf("Не удалось отправить визуализацию %s. Ошибка: %v",
				visualType, err))
		api.Send(errMsg)
		return
	}
}
