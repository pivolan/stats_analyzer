package plot

import (
	"fmt"
	"time"

	"github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
)

type dataDateForGraph struct {
	xValues     []float64
	yValues     []float64
	nameYAxis   string
	nameGraph   string
	typeRequest string
}

func NewDataDateForGraph(x []float64, y []float64, nameYAxis, nameGraph, typeRequest string) dataDateForGraph {
	return dataDateForGraph{
		xValues:     x,
		yValues:     y,
		nameYAxis:   nameYAxis,
		nameGraph:   nameGraph,
		typeRequest: typeRequest,
	}
}

func (d dataDateForGraph) GetNameGraph() string {
	return d.nameGraph
}
func (d dataDateForGraph) getNameYAxis() string {
	return d.nameYAxis
}
func (d dataDateForGraph) getYValues() []float64 {
	return d.yValues
}
func (d dataDateForGraph) getXValues() []float64 {
	return d.xValues
}
func (d dataDateForGraph) getDateAndHour() []string {
	data := make([]string, len(d.xValues))
	for i, v := range d.xValues {
		t := time.Unix(int64(v), 0)
		switch d.typeRequest {
		case "year":
			data[i] = fmt.Sprintf("%d", t.Year())
		case "month":
			data[i] = fmt.Sprintf("%d-%02d", t.Year(), t.Month())
		case "day":
			data[i] = fmt.Sprintf("%d-%02d-%02d", t.Year(), t.Month(), t.Day())
		case "hour":
			data[i] = fmt.Sprintf("%d-%02d-%02d %02d-%02d-%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
		default:
			return nil
		}
	}
	return data
}

func (d dataDateForGraph) getTypeRequest() string {
	return d.typeRequest
}
func (d dataDateForGraph) lenXValues() int {
	return len(d.xValues)
}

func (d dataDateForGraph) calculateChartDimensions(minBarWidth float64) (width, height int) {
	// Проверка входных параметров
	if len(d.yValues) == 0 || d.lenXValues() <= 0 || minBarWidth <= 0 {
		return 0, 0
	}
	x := 1.1
	if d.lenXValues() < 2 {
		x = 10.0
	} else if d.lenXValues() < 10 {
		x = 3.0
	}
	// // Находим максимальное значение для высоты

	// Константы для отступов и пропорций
	const (
		paddingY     = 100        // отступ для оси Y и подписей
		spacingRatio = 0.2        // соотношение отступа между столбцами к ширине столбца
		heightRatio  = 0.4        // коэффициент для добавления пространства сверху графика
		aspectRatio  = 9.0 / 16.0 // соотношение сторон по умолчанию
	)

	// Рассчитываем ширину
	barSpacing := minBarWidth * spacingRatio
	totalWidth := (minBarWidth+barSpacing)*float64(d.lenXValues()) + paddingY
	width = int(totalWidth*x) + paddingY
	height = int(float64(width) * aspectRatio)
	return width, height
}
func (d dataDateForGraph) generateBarValues() []chart.Value {
	maxVal := findMaxValue(d.yValues)
	var bars []chart.Value
	for i, v := range d.getDateAndHour() {
		if d.yValues[i] > maxVal {
			maxVal = d.getYValues()[i]
		}

		bars = append(bars, chart.Value{
			Value: d.getYValues()[i],
			Style: chart.Style{FillColor: drawing.ColorLime.WithAlpha(40),
				TextVerticalAlign: 100},
			Label: v,
		})
	}
	return bars
}

func (d dataDateForGraph) generateGrid() []chart.Tick {
	var ticks []chart.Tick
	max := findMaxValue(d.yValues)
	gridStep := calculateGridStep(max)
	for i := 0.0; i <= max; i += gridStep {
		ticks = append(ticks, chart.Tick{
			Value: i,
			Label: fmt.Sprintf("%.1f", i),
		})
	}
	return ticks
}
