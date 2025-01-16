// package main
package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
	"gorm.io/gorm"
)

func DrawBar(xStart []float64, xEnd []float64, yValues []float64) ([]byte, error) {

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

	// Настраиваем внешний вид графика
	graph := chart.BarChart{
		Title: "Distribution Histogram",
		Background: chart.Style{

			FillColor:   drawing.ColorWhite,
			StrokeColor: drawing.ColorBlue,
		},
		Height:   1024,
		Width:    2028,
		BarWidth: 30, // Уменьшаем ширину баров
		Bars:     bars,
		YAxis: chart.YAxis{
			Name: "Frequency",
		},
	}

	// Создаем буфер для записи изображения
	buffer := bytes.NewBuffer([]byte{})

	// Отрисовываем график в формате PNG
	err := graph.Render(chart.PNG, buffer)
	if err != nil {
		return nil, fmt.Errorf("error rendering chart: %v", err)
	}

	return buffer.Bytes(), nil
}

// Вспомогательная функция для нахождения максимального значения
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

func GenerateHistogram(db *gorm.DB, tableName ClickhouseTableName, columnName string) ([]byte, error, []byte) {
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

func DrawPlot(xValues []float64, yValues []float64) ([]byte, error) {
	graph := chart.Chart{
		Title: "Performance Test",
		XAxis: chart.XAxis{
			Name: "Time",
		},
		YAxis: chart.YAxis{
			Name: "Y asf",
			NameStyle: chart.Style{
				FontSize:  0,
				FontColor: chart.ColorBlue,
			},
			Style:          chart.Style{},
			Zero:           chart.GridLine{},
			AxisType:       1,
			Ascending:      true,
			ValueFormatter: nil,
			Range:          nil,
			TickStyle:      chart.Style{},
			Ticks:          nil,
			GridLines:      nil,
			GridMajorStyle: chart.Style{},
			GridMinorStyle: chart.Style{},
		},
		Background: chart.Style{
			Padding: chart.Box{
				Top:    50,
				Left:   25,
				Right:  25,
				Bottom: 10,
			},
			FillColor: drawing.ColorFromHex("efefef"),
		},
		Series: []chart.Series{
			// 1. Линейный график
			chart.ContinuousSeries{
				Name:            "Linear",
				Style:           chart.Style{StrokeColor: chart.ColorBlue},
				XValueFormatter: nil,
				YValueFormatter: nil,
				XValues:         xValues,
				YValues:         yValues,
			},
		},
	}

	// Добавляем легенду
	graph.Elements = []chart.Renderable{
		chart.Legend(&graph),
	}

	f, err := os.CreateTemp("./", "output_*.png")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	err = graph.Render(chart.PNG, f)
	if err != nil {
		return nil, err
	}
	f.Seek(0, 0)
	all, err := io.ReadAll(f)
	return all, err
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

	// Отрисовываем график в формате PNG
	err := graph.Render(chart.PNG, buffer)
	if err != nil {
		return nil, fmt.Errorf("error rendering chart: %v", err)
	}

	return buffer.Bytes(), nil
}
