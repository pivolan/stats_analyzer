package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
)

// func TestComplexChart(t *testing.T) {
// 	a, err := DrawPlot([]float64{1, 2, 3, 4, 5, 6}, []float64{1, 2, 3, 4, 5, 6})
// 	assert.NoError(t, err)
// 	err = os.WriteFile("output.png", a, 0655)
// 	assert.NoError(t, err)
// }

func TestDrawBar(t *testing.T) {

	a, err := DrawBar([]float64{1, 2, 3, 4, 5, 6}, []float64{1, 2, 3, 4, 5, 6}, []float64{1, 2, 3, 4, 5, 6})
	assert.NoError(t, err)
	err = os.WriteFile("output.png", a, 0655)
	assert.NoError(t, err)
}

func TestDrawHistogramm(t *testing.T) {

	bar := chart.BarChart{
		Title: "Test BarChart",
		TitleStyle: chart.Style{
			FontSize: 14.0,
		},
		Width:  1024,
		Height: 768,
		XAxis:  chart.Style{DotColor: chart.ColorRed},
		YAxis: chart.YAxis{
			Name: "Y Bar",
			NameStyle: chart.Style{
				FontSize:  0,
				FontColor: chart.ColorRed,
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
				Top:    40,
				Left:   20,
				Right:  20,
				Bottom: 40,
			},
			FillColor: drawing.ColorFromHex("efefef"),
		},

		Elements: []chart.Renderable{func(r chart.Renderer, canvasBox chart.Box, defaults chart.Style) {}},
	}

	file, err := os.Create("bar.png")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	err = bar.Render(chart.PNG, file)
	if err != nil {
		t.Fatal(err)
	}

}

func TestDrawDensityPlot(t *testing.T) {
	xValues := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	yValues := []float64{1, 2, 3, 4, 5, 6, 7, 8}

	b, err := DrawDensityPlot(xValues, yValues)
	assert.NoError(t, err)
	err = os.WriteFile("DrawDestinyPlot.png", b, 0655)
	assert.NoError(t, err)
}
