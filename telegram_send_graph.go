package main

import (
	"fmt"
	"log"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
)

// sendGraphVisualization отправляет визуализацию в чат с соответствующими пояснениями.
// Параметры:
//   - graph: байтовый массив с данными изображения
//   - visualType: тип визуализации (histogram, timeseries, density)
//   - columnName: имя анализируемой колонки
//   - chatID: ID чата для отправки
//   - api: экземпляр Telegram API для отправки сообщений
//   - timeUnit: единица измерения времени (опционально, для временных рядов)
func sendGraphVisualization(graph []byte, visualType string, columnName string, nameGraph string, chatID int64, api *tgbotapi.BotAPI, timeUnit ...string) {
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

	//176352
	//203362
	//150000
	// Создаем сообщение с изображением
	var maxSizePhoto = 150000

	switch {
	case maxSizePhoto > len(graph):
		docMsg := tgbotapi.NewPhotoUpload(chatID, pngFile)
		docMsg.Caption = generateVizualDescription(visualType, columnName, nameGraph, timeUnit...)

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
	case maxSizePhoto < len(graph):
		docMsg := tgbotapi.NewDocumentUpload(chatID, pngFile)
		docMsg.Caption = generateVizualDescription(visualType, columnName, nameGraph, timeUnit...)

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

}

func generateVizualDescription(description, columnName string, nameGraph string, timeUnit ...string) string {
	var caption string
	switch description {
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
			"Показывает %s.",
			columnName, timeUnitStr, nameGraph)
	default:
		caption = fmt.Sprintf("Визуализация данных: %s", columnName)
	}
	return caption
}
