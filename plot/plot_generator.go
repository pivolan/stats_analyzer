// package main
package plot

import (
	"bytes"
	"fmt"
	"math"

	"github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
)

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
				Bottom: 120,
			},
			FillColor: drawing.ColorWhite,
		},
		Width:  2048,
		Height: 1024,
		XAxis: chart.XAxis{
			Name:  "Values",
			Style: chart.Style{TextRotationDegrees: 88},
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

func DrawPlotBar(data dataForGraph) ([]byte, error) {

	var nameY, nameGraph string
	var ticks []chart.Tick
	barValues := data.generateBarValues()
	paddingX := customizePaddingXBottom(barValues)
	width, height := data.calculateChartDimensions(100)
	nameGraph = data.GetNameGraph()
	bar := chart.BarChart{}
	bar.Title = nameGraph
	bar.Background = chart.Style{
		FontSize:    160,
		StrokeColor: chart.ColorBlack,
		Padding: chart.Box{
			Bottom: paddingX,
			Top:    50,
		},
	}
	bar.Height = height + 50
	bar.Width = width + paddingX + 50
	bar.BarWidth = 60
	bar.Bars = barValues
	bar.YAxis = chart.YAxis{
		Name: nameY,
		Range: &chart.ContinuousRange{
			Min: 0.0,
			Max: findMaxValue(data.getYValues()),
		},
		Style: chart.Style{

			StrokeWidth: 2, // Толщина линии
			StrokeColor: chart.ColorBlack,
			FontSize:    17,
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
	}
	bar.XAxis = chart.Style{
		StrokeWidth:         2, // Толщина линии
		StrokeColor:         chart.ColorBlack,
		TextRotationDegrees: 88,
		FontSize:            17,
	}
	buffer := bytes.NewBuffer([]byte{})

	// Отрисовываем график в формате PNG
	err := bar.Render(chart.PNG, buffer)
	if err != nil {
		return nil, fmt.Errorf("error rendering chart: %v", err)
	}

	return buffer.Bytes(), nil
}
func findMaxValue(y []float64) float64 {
	if len(y) == 0 {
		return 0
	}
	max := y[0]
	for _, v := range y {
		if v > max {
			max = v
		}
	}
	return max
}

func customizePaddingXBottom(values []chart.Value) int {
	count := 0
	for _, v := range values {
		if len(v.Label) > count {
			count = len(v.Label)
		}
	}
	return int(count * 8)
}
