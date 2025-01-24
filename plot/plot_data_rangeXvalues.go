package plot

import (
	"fmt"

	"github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
)

type dataRangeXValuesForGraph struct {
	xStart, xEnd []float64
	yValues      []float64
	nameYAxis    string
	nameGraph    string
	typeRequest  string
}

func NewDataRangeXValuesForGraph(xStart, xEnd, y []float64, nameYAxis, nameGraph, typeRequest string) dataRangeXValuesForGraph {
	return dataRangeXValuesForGraph{
		xStart:      xStart,
		xEnd:        xEnd,
		yValues:     y,
		nameYAxis:   nameYAxis,
		nameGraph:   nameGraph,
		typeRequest: typeRequest,
	}
}
func (d dataRangeXValuesForGraph) GetNameGraph() string {
	return d.nameGraph
}
func (d dataRangeXValuesForGraph) getNameYAxis() string {
	return d.nameYAxis
}
func (d dataRangeXValuesForGraph) getYValues() []float64 {
	return d.yValues
}
func (d dataRangeXValuesForGraph) getXValues() ([]float64, []float64) {
	return d.xStart, d.xEnd
}

func (d dataRangeXValuesForGraph) lenXValues() int {
	return len(d.xStart)
}
func (d dataRangeXValuesForGraph) findMaxValue() float64 {
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
func (d dataRangeXValuesForGraph) calculateChartDimensions(minBarWidth float64) (width, height int) {
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

func (d dataRangeXValuesForGraph) generateBarValues() []chart.Value {
	var bars []chart.Value
	yValues := d.getYValues()
	max := d.findMaxValue()
	xStart, xEnd := d.getXValues()
	for i := 0; i < len(d.xStart); i++ {
		if yValues[i] > max {
			max = yValues[i]
		}
		bars = append(bars, chart.Value{
			Value: yValues[i],
			Label: fmt.Sprintf("%.f-%.f", xStart[i], xEnd[i]), // Округляем до 1 знака после запятой
			Style: chart.Style{
				FillColor: drawing.ColorPurple.WithAlpha(100),
			},
		})
	}
	return bars
}

func (d dataRangeXValuesForGraph) generateGrid() []chart.Tick {
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
