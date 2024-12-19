// number_analyzer.go
package main

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type NumberStats struct {
	Average float64
	Median  float64
	Min     float64
	Max     float64
	Count   int
}

// ExtractNumbers извлекает числа из текста, поддерживая различные разделители
func ExtractNumbers(text string) []float64 {
	// Заменяем запятые и переносы строк на пробелы
	text = strings.ReplaceAll(text, ",", " ")
	text = strings.ReplaceAll(text, "\n", " ")

	// Регулярное выражение для поиска чисел (включая отрицательные и десятичные)
	re := regexp.MustCompile(`-?\d*\.?\d+`)
	matches := re.FindAllString(text, -1)

	numbers := make([]float64, 0, len(matches))
	for _, match := range matches {
		if num, err := strconv.ParseFloat(match, 64); err == nil {
			numbers = append(numbers, num)
		}
	}
	return numbers
}

// AnalyzeNumbers вычисляет статистические метрики для массива чисел
func AnalyzeNumbers(numbers []float64) *NumberStats {
	if len(numbers) == 0 {
		return nil
	}

	// Копируем массив для сортировки (для медианы)
	sorted := make([]float64, len(numbers))
	copy(sorted, numbers)
	sort.Float64s(sorted)

	// Вычисляем среднее
	sum := 0.0
	for _, num := range numbers {
		sum += num
	}
	avg := sum / float64(len(numbers))

	// Вычисляем медиану
	var median float64
	if len(sorted)%2 == 0 {
		median = (sorted[len(sorted)/2-1] + sorted[len(sorted)/2]) / 2
	} else {
		median = sorted[len(sorted)/2]
	}

	return &NumberStats{
		Average: roundToTwo(avg),
		Median:  roundToTwo(median),
		Min:     roundToTwo(sorted[0]),
		Max:     roundToTwo(sorted[len(sorted)-1]),
		Count:   len(numbers),
	}
}

// FormatStats форматирует статистику для вывода в Telegram
func FormatStats(stats *NumberStats) string {
	if stats == nil {
		return "❌ Числа не найдены в сообщении"
	}

	return fmt.Sprintf(`📊 Статистика чисел:

Количество: %d
Среднее: %.2f
Медиана: %.2f
Минимум: %.2f
Максимум: %.2f`,
		stats.Count,
		stats.Average,
		stats.Median,
		stats.Min,
		stats.Max)
}

// roundToTwo округляет
func roundToTwo(num float64) float64 {
	return math.Round(num*100) / 100
}
