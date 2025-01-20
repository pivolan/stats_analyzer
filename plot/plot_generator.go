// package main
package plot

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/pivolan/stats_analyzer/domain/models"
	"github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
	"gorm.io/gorm"
)

func DrawTimeSeries(xValues []float64, yValues []float64) ([]byte, error) {

	var ticks []chart.Tick

	var bars []chart.Value
	maxVal := 0.0

	for i := 0; i < len(xValues); i++ {
		if yValues[i] > maxVal {
			maxVal = yValues[i]
		}
		t := time.Unix(int64(xValues[i]), 0)

		bars = append(bars, chart.Value{
			Value: yValues[i],
			Style: chart.Style{FillColor: drawing.ColorLime.WithAlpha(40)},
			Label: fmt.Sprintf("%d-%02d-%02d", t.Year(), t.Month(), t.Day()),
		})
	}
	maxY := findMaxValue(yValues)
	gridStep := calculateGridStep(maxY)
	width, height := calculateChartDimensions(yValues, len(xValues), 120, 350)
	for i := 0.0; i <= maxY; i += gridStep {
		ticks = append(ticks, chart.Tick{
			Value: i,
			Label: fmt.Sprintf("%.1f", i),
		})
	}
	// Настраиваем внешний вид графика
	bar := chart.BarChart{
		Title:      "",
		TitleStyle: chart.Style{Hidden: true},

		Background: chart.Style{

			Padding: chart.Box{
				Top: 40,

				Bottom: 40,
			}},
		Height: height,
		Width:  width,

		BarWidth: 45, // Уменьшаем ширину баров
		Bars:     bars,
		XAxis: chart.Style{
			StrokeWidth: 2, // Толщина линии
			StrokeColor: chart.ColorBlack,
		},
		YAxis: chart.YAxis{
			Name: "Count",
			Range: &chart.ContinuousRange{
				Min: 0.0,
				Max: maxY,
			},

			Style: chart.Style{

				StrokeWidth: 2, // Толщина линии
				StrokeColor: chart.ColorBlack,
			},

			Ticks: ticks,
			GridMinorStyle: chart.Style{
				StrokeColor: chart.ColorBlack,
				StrokeWidth: 1,
				DotWidth:    1,
			},
			GridMajorStyle: chart.Style{
				StrokeColor:     chart.ColorBlack,
				StrokeWidth:     1,
				DotWidth:        1,
				StrokeDashArray: []float64{5.0, 5.0}, // Пунктирная линия
			},
		},
	}
	// Надо не забыть убрать  перед деплоем!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	f, _ := os.Create("dynamic_grid.png")
	defer f.Close()
	bar.Render(chart.PNG, f)
	// Создаем буфер для записи изображения
	buffer := bytes.NewBuffer([]byte{})

	// Отрисовываем график в формате PNG
	err := bar.Render(chart.PNG, buffer)
	if err != nil {
		return nil, fmt.Errorf("error rendering chart: %v", err)
	}

	return buffer.Bytes(), nil
}
func DrawBar(xStart []float64, xEnd []float64, yValues []float64) ([]byte, error) {
	var ticks []chart.Tick

	var bars []chart.Value
	maxVal := 0.0

	for i := 0; i < len(xStart); i++ {
		if yValues[i] > maxVal {
			maxVal = yValues[i]
		}
		bars = append(bars, chart.Value{
			Value: yValues[i],
			Label: fmt.Sprintf("%.f-%.f", xStart[i], xEnd[i]), // Округляем до 1 знака после запятой

		})
	}
	maxY := findMaxValue(yValues)
	gridStep := calculateGridStep(maxY)
	width, height := calculateChartDimensions(yValues, len(xStart), 60, 400)
	for i := 0.0; i <= maxY; i += gridStep {
		ticks = append(ticks, chart.Tick{
			Value: i,
			Label: fmt.Sprintf("%.1f", i),
		})
	}
	// Настраиваем внешний вид графика
	bar := chart.BarChart{
		Title:      "",
		TitleStyle: chart.Style{Hidden: true},
		Background: chart.Style{

			Padding: chart.Box{
				Top: 40,
			}},
		Height:   height,
		Width:    width,
		BarWidth: 45, // Уменьшаем ширину баров
		Bars:     bars,
		XAxis: chart.Style{
			StrokeWidth: 2, // Толщина линии
			StrokeColor: chart.ColorBlack,
		},
		YAxis: chart.YAxis{
			Name: "Frequency",
			Range: &chart.ContinuousRange{
				Min: 0.0,
				Max: maxY,
			},

			Style: chart.Style{

				StrokeWidth: 2, // Толщина линии
				StrokeColor: chart.ColorBlack,
			},

			Ticks: ticks,
			GridMinorStyle: chart.Style{
				StrokeColor: chart.ColorBlack,
				StrokeWidth: 1,
				DotWidth:    1,
			},
			GridMajorStyle: chart.Style{
				StrokeColor:     chart.ColorBlack,
				StrokeWidth:     1,
				DotWidth:        1,
				StrokeDashArray: []float64{5.0, 5.0}, // Пунктирная линия
			},
		},
	}

	// Создаем буфер для записи изображения
	buffer := bytes.NewBuffer([]byte{})

	// Отрисовываем график в формате PNG
	err := bar.Render(chart.PNG, buffer)
	if err != nil {
		return nil, fmt.Errorf("error rendering chart: %v", err)
	}

	return buffer.Bytes(), nil
}

func GenerateHistogram(db *gorm.DB, tableName models.ClickhouseTableName, columnName string) ([]byte, error, []byte) {
	// SQL запрос для получения гистограммы
	histogramSQL := fmt.Sprintf(`
        WITH 
            min_max AS (
                SELECT min(%[1]s) as min_val, max(%[1]s) as max_val 
                FROM %[2]s 
                WHERE %[1]s IS NOT NULL
            ),
            hist AS (
                SELECT 
                    arrayJoin(histogram(20)(%[1]s)) as hist_row
                FROM %[2]s
            )
        SELECT 
            hist_row.1 as range_start,
            hist_row.2 as range_end,
            hist_row.3 as count
        FROM hist
        ORDER BY range_start;
    `, columnName, tableName)

	// Определяем структуру с корректными тегами
	type HistogramData struct {
		RangeStart float64 `db:"range_start"`
		RangeEnd   float64 `db:"range_end"`
		Count      float64 `db:"count"`
	}

	var histData []HistogramData
	if err := db.Raw(histogramSQL).Scan(&histData).Error; err != nil {
		return nil, fmt.Errorf("error getting histogram data: %v", err), nil
	}
	xStart := make([]float64, len(histData))
	xEnd := make([]float64, len(histData))
	// Логируем полученные данные
	log.Printf("Received %d data points", len(histData))
	for i, data := range histData {
		log.Printf("Data point %d: Start=%f, End=%f, Count=%f",
			i, data.RangeStart, data.RangeEnd, data.Count)
	}

	// Подготавливаем данные для визуализации
	xValues := make([]float64, len(histData))
	yValues := make([]float64, len(histData))

	for i, data := range histData {
		// Используем середину диапазона для оси X
		xStart[i] = data.RangeStart
		xEnd[i] = data.RangeEnd
		yValues[i] = data.Count
		xValues[i] = (xStart[i] + xEnd[i]) / 2
		log.Printf("Point %d: XStart=%f,XEnd:=%f ,Y=%f", i, xStart[i], xEnd[i], yValues[i])
	}

	// Генерируем гистограмму
	hist, _ := DrawBar(xStart, xEnd, yValues)
	graph, _ := DrawDensityPlot(xValues, yValues)
	return hist, nil, graph
}

func DrawDensityPlot(xValues []float64, yValues []float64) ([]byte, error) {
	// Нормализуем значения y, чтобы площадь под кривой была равна 1
	totalArea := 0.0
	binWidth := 0.0
	if len(xValues) > 1 {
		binWidth = xValues[1] - xValues[0]
	}

	for _, y := range yValues {
		totalArea += y * binWidth
	}

	normalizedY := make([]float64, len(yValues))
	for i, y := range yValues {
		normalizedY[i] = y / totalArea
	}

	// Создаем серию точек для графика
	series := &chart.ContinuousSeries{
		XValues: xValues,
		YValues: normalizedY,
		Style: chart.Style{
			StrokeColor: drawing.ColorBlue,
			StrokeWidth: 2,
			Hidden:      false,
		},
	}

	// Создаем область под кривой
	fillSeries := &chart.ContinuousSeries{
		XValues: xValues,
		YValues: normalizedY,
		Style: chart.Style{
			StrokeColor: drawing.ColorBlue,
			FillColor:   drawing.ColorRed.WithAlpha(100),
			StrokeWidth: 0,
			Hidden:      false,
		},
	}

	// Настраиваем график
	graph := chart.Chart{
		Title: "Density Distribution",
		Background: chart.Style{
			Padding: chart.Box{
				Top:    40,
				Left:   20,
				Right:  20,
				Bottom: 20,
			},
			FillColor: drawing.ColorWhite,
		},
		Width:  2048,
		Height: 1024,
		XAxis: chart.XAxis{
			Name: "Values",

			ValueFormatter: func(v interface{}) string {
				if vf, isFloat := v.(float64); isFloat {
					return fmt.Sprintf("%.1f", vf)
				}
				return ""
			},
		},
		YAxis: chart.YAxis{
			Name: "Density",

			ValueFormatter: func(v interface{}) string {
				if vf, isFloat := v.(float64); isFloat {
					return fmt.Sprintf("%.6f", vf)
				}
				return ""
			},
		},
		Series: []chart.Series{
			fillSeries, // Сначала рисуем заполнение
			series,     // Потом линию
		},
	}

	// Создаем буфер для записи изображения
	buffer := bytes.NewBuffer([]byte{})
	graph.Background.StrokeWidth = 1
	graph.Background.StrokeColor = drawing.ColorFromHex("efefef")

	// Отрисовываем график в формате PNG
	err := graph.Render(chart.PNG, buffer)
	if err != nil {
		return nil, fmt.Errorf("error rendering chart: %v", err)
	}

	return buffer.Bytes(), nil
}

func DrawBarXString(x []string, y []float64) ([]byte, error) {
	var bars chart.Values
	var ticks []chart.Tick
	width, Height := calculateChartDimensions(y, len(x), 60, 360)
	maxValue := findMaxValue(y)
	gridStep := calculateGridStep(maxValue)
	maxY := math.Ceil(maxValue/gridStep) * gridStep
	for i := 0.0; i <= maxY; i += gridStep {
		ticks = append(ticks, chart.Tick{
			Value: i,
			Label: fmt.Sprintf("%.1f", i),
		})
	}
	for i := range x {

		bars = append(bars, chart.Value{
			Label: x[i],
			Value: y[i],
		})

	}

	// Настраиваем внешний вид графика
	bar := chart.BarChart{
		Title:      "Histogram String Value",
		TitleStyle: chart.Style{Hidden: true},
		Background: chart.Style{

			Padding: chart.Box{
				Top: 40,
			}},
		Height:   Height,
		Width:    width,
		BarWidth: 30, // Уменьшаем ширину баров
		Bars:     bars,
		XAxis: chart.Style{
			StrokeWidth: 2, // Толщина линии
			StrokeColor: chart.ColorBlack,
		},
		YAxis: chart.YAxis{
			Name: "Frequency",
			Range: &chart.ContinuousRange{
				Min: 0.0,
				Max: maxY,
			},

			Style: chart.Style{

				StrokeWidth: 2, // Толщина линии
				StrokeColor: chart.ColorBlack,
			},

			Ticks: ticks,
			GridMinorStyle: chart.Style{
				StrokeColor: chart.ColorBlack,
				StrokeWidth: 1,
				DotWidth:    1,
			},
			GridMajorStyle: chart.Style{
				StrokeColor:     chart.ColorBlack,
				StrokeWidth:     1,
				DotWidth:        1,
				StrokeDashArray: []float64{5.0, 5.0}, // Пунктирная линия

			},
		},
	}

	// Создаем буфер для записи изображения
	buffer := bytes.NewBuffer([]byte{})

	// Отрисовываем график в формате PNG
	err := bar.Render(chart.PNG, buffer)
	if err != nil {
		return nil, fmt.Errorf(" error rendering chart: %v", err)
	}

	return buffer.Bytes(), nil

}
func GenerateHistogramForString(db *gorm.DB, tableName models.ClickhouseTableName, columnName string) ([]byte, error) {
	// SQL для получения категориальных данных с подсчетом
	categoricalSQL := fmt.Sprintf(`
    SELECT %[1]s as category, 
           COUNT(*) as count
    FROM %[2]s
    WHERE %[1]s IS NOT NULL AND %[1]s != ''
    GROUP BY %[1]s
    ORDER BY count DESC
    LIMIT 20
`, columnName, tableName)

	type CategoryCount struct {
		Category string
		Count    int64
	}

	var categoryCounts []CategoryCount
	if err := db.Raw(categoricalSQL).Scan(&categoryCounts).Error; err != nil {
		return nil, fmt.Errorf("error getting category counts: %v", err)
	}

	// Подготовка данных для графика
	categories := make([]string, len(categoryCounts))
	counts := make([]float64, len(categoryCounts))
	maxCount := float64(0)

	for i, cc := range categoryCounts {
		categories[i] = cc.Category
		counts[i] = float64(cc.Count)
		if counts[i] > maxCount {
			maxCount = counts[i]
		}
	}

	return DrawBarXString(categories, counts)
}

func calculateGridStep(maxValue float64) float64 {
	// Проверка на корректность входного значения
	if maxValue <= 0 {
		return 0
	}

	// Обработка очень маленьких чисел
	if maxValue < 1e-10 {
		return 1e-10
	}

	// Находим порядок величины максимального значения
	magnitude := math.Pow(10, math.Floor(math.Log10(maxValue)))

	// Нормализуем значение к диапазону [1, 10)
	normalized := maxValue / magnitude

	// Определяем базовые шаги для разных диапазонов
	// Для больших чисел используем более крупные шаги
	var step float64
	switch {
	case magnitude >= 1000:
		// Для значений >= 1000 используем шаги по 1000, 2000, 5000
		switch {
		case normalized <= 1:
			step = 0.2 // даст шаг 200 для тысяч
		case normalized <= 2:
			step = 0.5 // даст шаг 500 для тысяч
		case normalized <= 5:
			step = 1.0 // даст шаг 1000 для тысяч
		default:
			step = 2.0 // даст шаг 2000 для тысяч
		}
	case magnitude >= 100:
		// Для значений >= 100 используем шаги по 100, 200, 500
		switch {
		case normalized <= 1:
			step = 0.2
		case normalized <= 2:
			step = 0.5
		case normalized <= 5:
			step = 1.0
		default:
			step = 2.0
		}
	default:
		// Для меньших значений используем стандартные шаги
		switch {
		case normalized <= 1:
			step = 0.2
		case normalized <= 2:
			step = 0.5
		case normalized <= 5:
			step = 1.0
		default:
			step = 2.0
		}
	}

	// Возвращаем окончательный шаг с учетом порядка величины
	finalStep := step * magnitude

	// Округляем большие шаги до "красивых" чисел
	if finalStep >= 1000 {
		// Округляем до сотен для тысяч
		return math.Round(finalStep/100) * 100
	}
	if finalStep >= 100 {
		// Округляем до десятков для сотен
		return math.Round(finalStep/10) * 10
	}

	return finalStep
}
func calculateChartDimensions(values []float64, numBars int, minBarWidth, minHeight float64) (width, height int) {
	// Проверка входных параметров
	if len(values) == 0 || numBars <= 0 || minBarWidth <= 0 {
		return 0, 0
	}
	if len(values) < 10 {
		return 600, 400
	}
	// Находим максимальное значение для высоты
	max := findMaxValue(values)
	if max <= 0 {
		return 0, 0
	}

	// Константы для отступов и пропорций
	const (
		paddingY     = 100        // отступ для оси Y и подписей
		spacingRatio = 0.2        // соотношение отступа между столбцами к ширине столбца
		heightRatio  = 1.2        // коэффициент для добавления пространства сверху графика
		aspectRatio  = 16.0 / 9.0 // соотношение сторон по умолчанию
	)

	// Рассчитываем ширину
	barSpacing := minBarWidth * spacingRatio
	totalWidth := (minBarWidth+barSpacing)*float64(numBars) + barSpacing
	width = int(totalWidth) + paddingY

	// Рассчитываем высоту
	calculatedHeight := max * heightRatio
	if minHeight <= 0 {
		minHeight = totalWidth / aspectRatio
	}

	// Используем большее значение из минимальной и расчётной высоты
	targetHeight := math.Max(minHeight, calculatedHeight)

	// Округляем высоту до ближайшего большего числа, кратного 50
	height = int(math.Ceil(targetHeight/50.0) * 50)

	return width, height
}
func findMaxValue(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
}
