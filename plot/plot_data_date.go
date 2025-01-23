package plot

import (
	"fmt"
	"time"
)

type dataForGraph struct {
	xValues     []float64
	yValues     []float64
	nameYAxis   string
	nameGraph   string
	typeRequest string
}

func NewDataForGraph(x []float64, y []float64, nameYAxis, nameGraph, typeRequest string) dataForGraph {
	return dataForGraph{
		xValues:     x,
		yValues:     y,
		nameYAxis:   nameYAxis,
		nameGraph:   nameGraph,
		typeRequest: typeRequest,
	}
}

func (d *dataForGraph) GetNameGraph() string {
	return d.nameGraph
}
func (d *dataForGraph) getNameYAxis() string {
	return d.nameYAxis
}
func (d *dataForGraph) getYValues() []float64 {
	return d.yValues
}
func (d *dataForGraph) getDateAndHour() []string {
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

func (d *dataForGraph) lenXValues() int {
	return len(d.xValues)
}
func (d *dataForGraph) findMaxValue() float64 {
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
func (d *dataForGraph) calculateChartDimensions(minBarWidth float64) (width, height int) {
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
