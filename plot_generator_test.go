package main

import (
	"github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
	"os"
	"testing"
)

func TestComplexChart(t *testing.T) {
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
				XValues:         []float64{3, 2, 3, 4, 5, 6, 7, 8},
				YValues:         []float64{1, 2, 3, 4, 5, 5, 2, 1},
			},
		},
	}

	// Добавляем легенду
	graph.Elements = []chart.Renderable{
		chart.Legend(&graph),
	}

	f, err := os.Create("output.png")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	err = graph.Render(chart.PNG, f)
	if err != nil {
		t.Fatal(err)
	}
}
