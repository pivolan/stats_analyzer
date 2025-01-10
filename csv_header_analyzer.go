// csv_header_analyzer.go
package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

type HeaderAnalysis struct {
	Headers        []string // Итоговые заголовки
	FirstRowIsData bool     // Является ли первая строка данными
	FirstDataRow   []string // Первая строка с данными
}

// AnalyzeHeaders анализирует первую строку CSV и определяет структуру заголовков
func AnalyzeHeaders(firstRow []string) *HeaderAnalysis {
	if len(firstRow) == 0 {
		return nil
	}

	result := &HeaderAnalysis{
		Headers:        make([]string, len(firstRow)),
		FirstRowIsData: false,
		FirstDataRow:   firstRow,
	}

	// Подсчитываем, сколько полей похожи на заголовки
	headerLikeCount := 0
	for _, field := range firstRow {
		if isLikelyHeader(field) {
			headerLikeCount++
		}
	}

	// Если большинство полей похожи на заголовки
	if float64(headerLikeCount)/float64(len(firstRow)) >= 0.5 {
		result.FirstRowIsData = false
		// Очищаем существующие заголовки
		for i, header := range firstRow {

			result.Headers[i] = cleanHeaderName(header, i)

		}
	} else {
		// Генерируем заголовки для данных
		result.FirstRowIsData = true
		for i := range firstRow {
			result.Headers[i] = generateColumnName(i)
		}
	}

	// Проверяем на дубликаты
	result.Headers = ValidateHeaders(result.Headers)
	return result
}

// isLikelyHeader определяет, похож ли текст на заголовок
func isLikelyHeader(text string) bool {
	text = strings.TrimSpace(text)
	if text == "" {
		return false
	}

	// Проверяем, не является ли значение числом
	if _, err := strconv.ParseFloat(text, 64); err == nil {
		return false
	}

	// Проверяем на типичные форматы дат
	datePatterns := []string{
		`^\d{4}-\d{2}-\d{2}$`,
		`^\d{2}/\d{2}/\d{4}$`,
		`^\d{2}\.\d{2}\.\d{4}$`,
		`^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$`,
		`^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d+$`,
	}

	for _, pattern := range datePatterns {
		if matched, _ := regexp.MatchString(pattern, text); matched {
			return false
		}
	}

	// Подсчитываем буквы и специальные символы
	letters := 0
	digits := 0
	spaces := 0
	specials := 0

	for _, r := range text {
		switch {
		case unicode.IsLetter(r):
			letters++
		case unicode.IsDigit(r):
			digits++
		case unicode.IsSpace(r):
			spaces++
		default:
			specials++
		}
	}

	// Заголовок должен содержать буквы и может содержать цифры и спецсимволы
	totalChars := letters + digits + specials
	if totalChars == 0 {
		return false
	}

	// Если букв больше 30% от всех символов - вероятно это заголовок
	return letters > 0 && float64(letters)/float64(totalChars) >= 0.3
}

// generateColumnName создает имя столбца по индексу
func generateColumnName(index int) string {
	return fmt.Sprintf("column_%d", index+1)
}

// ValidateHeaders проверяет и исправляет дубликаты в заголовках
func ValidateHeaders(headers []string) []string {
	seen := make(map[string]int)
	result := make([]string, len(headers))

	for i, header := range headers {
		originalHeader := header
		counter := 1

		// Пока находим дубликаты, добавляем счетчик к имени
		for {
			if count, exists := seen[header]; exists {
				header = fmt.Sprintf("%s_%d", originalHeader, counter)
				counter++
			} else {
				seen[header] = count + 1
				break
			}
		}

		result[i] = header
	}

	return result
}

// isNumericData проверяет, похожи ли данные на числовые значения
func isNumericData(values []string) bool {
	numericCount := 0
	for _, value := range values {
		if _, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
			numericCount++
		}
	}
	return float64(numericCount)/float64(len(values)) >= 0.8
}

// cleanHeaderName очищает и форматирует имя заголовка
func cleanHeaderName(header string, index int) string {
	header = strings.TrimSpace(header)
	if header == "" {
		return generateColumnName(index)
	}

	// Используем существующую функцию replaceSpecialSymbols для совместимости с существующими запросами
	cleaned := replaceSpecialSymbols(header)

	if cleaned == "" {
		return generateColumnName(index)
	}
	if !isLikelyHeader(header) {
		return generateColumnName(index)
	}
	// Приводим к нижнему регистру для консистентности
	return strings.ToLower(cleaned)
}

// isDateData проверяет, похожи ли данные на даты
func isDateData(values []string) bool {
	dateCount := 0
	patterns := []string{
		`^\d{4}-\d{2}-\d{2}$`,
		`^\d{2}/\d{2}/\d{4}$`,
		`^\d{2}\.\d{2}\.\d{4}$`,
		`^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$`,
	}

	for _, value := range values {
		for _, pattern := range patterns {
			if matched, _ := regexp.MatchString(pattern, strings.TrimSpace(value)); matched {
				dateCount++
				break
			}
		}
	}
	return float64(dateCount)/float64(len(values)) >= 0.8
}
