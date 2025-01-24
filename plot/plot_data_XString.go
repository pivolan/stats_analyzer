package plot

import (
	"fmt"

	"github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
)

type dataXStringsForGraph struct {
	xValues     []string
	yValues     []float64
	nameYAxis   string
	nameGraph   string
	typeRequest string
}

func NewDataXStringsForGraph(xValues []string, y []float64, nameYAxis, nameGraph, typeRequest string) dataXStringsForGraph {
	return dataXStringsForGraph{
		xValues: xValues,

		yValues:     y,
		nameYAxis:   nameYAxis,
		nameGraph:   nameGraph,
		typeRequest: typeRequest,
	}
}
func (d dataXStringsForGraph) GetNameGraph() string {
	return d.nameGraph
}
func (d dataXStringsForGraph) getNameYAxis() string {
	return d.nameYAxis
}
func (d dataXStringsForGraph) getYValues() []float64 {
	return d.yValues
}
func (d dataXStringsForGraph) getXValues() []string {
	return d.xValues
}

func (d dataXStringsForGraph) lenXValues() int {
	return len(d.xValues)
}
func (d dataXStringsForGraph) findMaxValue() float64 {
	if len(d.yValues) == 0 {
		return 0
	}
	max := d.yValues[0]
	for _, v := range d.yValues {
		if v > max {
			max = v
		}
	}
	return max
}
func (d dataXStringsForGraph) calculateChartDimensions(minBarWidth float64) (width, height int) {
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

func (d dataXStringsForGraph) generateBarValues() []chart.Value {
	var bars []chart.Value
	yValues := d.getYValues()
	max := d.findMaxValue()
	xValues := d.getXValues()
	for i := 0; i < len(d.xValues); i++ {
		if yValues[i] > max {
			max = yValues[i]
		}
		bars = append(bars, chart.Value{
			Value: yValues[i],
			Label: fmt.Sprintf("%s", xValues[i]), // Округляем до 1 знака после запятой
			Style: chart.Style{
				FillColor: drawing.ColorPurple.WithAlpha(100),
			},
		})
	}
	return bars
}

func (d dataXStringsForGraph) generateGrid() []chart.Tick {
	var ticks []chart.Tick
	max := d.findMaxValue()
	gridStep := calculateGridStep(max)
	for i := 0.0; i <= max; i += gridStep {
		ticks = append(ticks, chart.Tick{
			Value: i,
			Label: fmt.Sprintf("%.1f", i),
		})
	}

	return ticks
}
