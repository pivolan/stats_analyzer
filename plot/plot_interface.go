package plot

import "github.com/wcharczuk/go-chart/v2"

type dataForGraph interface {
	GetNameGraph() string
	getNameYAxis() string
	getYValues() []float64
	calculateChartDimensions(float64) (int, int)
	generateBarValues() []chart.Value
}
