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
	Average   float64
	Median    float64
	Min       float64
	Max       float64
	Count     int
	Quantiles map[float64]float64 // Добавляем поле для хранения квантилей
	IQR       float64             // Межквартильный размах
	Outliers  []float64           // Выбросы
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

// calculateQuantile вычисляет квантиль заданного уровня
func calculateQuantile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}

	// Вычисляем позицию квантиля
	pos := p * float64(len(sorted)-1)
	floor := math.Floor(pos)
	ceil := math.Ceil(pos)

	if floor == ceil {
		return sorted[int(pos)]
	}

	// Интерполяция между двумя ближайшими значениями
	lower := sorted[int(floor)]
	upper := sorted[int(ceil)]
	fraction := pos - floor

	return lower + fraction*(upper-lower)
}

// findOutliers находит выбросы на основе межквартильного размаха
func findOutliers(numbers []float64, q1 float64, q3 float64, iqr float64) []float64 {
	outliers := make([]float64, 0)
	lowerBound := q1 - 1.5*iqr
	upperBound := q3 + 1.5*iqr

	for _, num := range numbers {
		if num < lowerBound || num > upperBound {
			outliers = append(outliers, num)
		}
	}
	return outliers
}

// AnalyzeNumbers вычисляет статистические метрики для массива чисел
func AnalyzeNumbers(numbers []float64) *NumberStats {
	if len(numbers) == 0 {
		return nil
	}

	// Копируем массив для сортировки
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

	// Вычисляем квантили
	quantiles := make(map[float64]float64)
	quantileList := []float64{0.01, 0.025, 0.1, 0.25, 0.75, 0.9, 0.975, 0.99} // 1%, 2.5%, 10%, 25%, 75%, 90%, 97.5%, 99% квантили

	for _, p := range quantileList {
		quantiles[p] = roundToTwo(calculateQuantile(sorted, p))
	}

	// Вычисляем межквартильный размах (IQR)
	iqr := quantiles[0.75] - quantiles[0.25]

	// Находим выбросы
	outliers := findOutliers(numbers, quantiles[0.25], quantiles[0.75], iqr)

	return &NumberStats{
		Average:   roundToTwo(avg),
		Median:    roundToTwo(median),
		Min:       roundToTwo(sorted[0]),
		Max:       roundToTwo(sorted[len(sorted)-1]),
		Count:     len(numbers),
		Quantiles: quantiles,
		IQR:       roundToTwo(iqr),
		Outliers:  outliers,
	}
}

// FormatStats форматирует статистику для вывода в Telegram
func FormatStats(stats *NumberStats) string {
	if stats == nil {
		return "❌ Числа не найдены в сообщении"
	}

	outlierStr := ""
	if len(stats.Outliers) > 0 {
		outlierStr = fmt.Sprintf("\nВыбросы: %.2f", stats.Outliers)
	}

	return fmt.Sprintf(`📊 Статистика чисел:

Количество: %d
Среднее: %.2f
Медиана: %.2f
Минимум: %.2f
Максимум: %.2f

Экстремальные квантили:
1-й процентиль: %.2f
2.5-й процентиль: %.2f
97.5-й процентиль: %.2f
99-й процентиль: %.2f

Основные квантили:
10-й процентиль: %.2f
25-й процентиль (Q1): %.2f
75-й процентиль (Q3): %.2f
90-й процентиль: %.2f

Межквартильный размах (IQR): %.2f%s`,
		stats.Count,
		stats.Average,
		stats.Median,
		stats.Min,
		stats.Max,
		stats.Quantiles[0.01],
		stats.Quantiles[0.025],
		stats.Quantiles[0.975],
		stats.Quantiles[0.99],
		stats.Quantiles[0.1],
		stats.Quantiles[0.25],
		stats.Quantiles[0.75],
		stats.Quantiles[0.9],
		stats.IQR,
		outlierStr)
}

// roundToTwo округляет число до двух знаков после запятой
func roundToTwo(num float64) float64 {
	return math.Round(num*100) / 100
}
