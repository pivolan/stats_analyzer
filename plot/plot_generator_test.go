package plot

import (
	"fmt"
	"math"
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

func TestDrawBarXstring(t *testing.T) {

	xValues := []string{"D", "a", "v", "c", "e"}
	yValues := []float64{10, 10, 10, 10, 10, 10, 10, 10}

	b, err := DrawBarXString(xValues, yValues)
	assert.NoError(t, err)
	err = os.WriteFile("DrawBarstring.png", b, 0655)
	assert.NoError(t, err)
}
func TestDrawBarXstring1(t *testing.T) {
	xValues := []float64{1, 2, 3, 4, 5}
	yValues := []float64{10, 10, 10, 10, 10}
	// Создаем столбчатую диаграмму
	bars := chart.BarChart{
		Title: "Пример столбчатой диаграммы",

		Background: chart.Style{
			Padding: chart.Box{
				Top: 40,
			},
		},
		Height:   400,
		BarWidth: 60, // Ширина столбцов
		Bars: []chart.Value{
			{Value: yValues[0], Label: fmt.Sprintf("%f", xValues[0])},
			{Value: yValues[1], Label: fmt.Sprintf("%f", xValues[1])},
			{Value: yValues[2], Label: fmt.Sprintf("%f", xValues[2])},
			{Value: yValues[3], Label: fmt.Sprintf("%f", xValues[3])},
			{Value: yValues[4], Label: fmt.Sprintf("%f", xValues[4])},
		},
		YAxis: chart.YAxis{
			Range: &chart.ContinuousRange{
				Min: 0.0,
				Max: yValues[4],
			},
			// Настраиваем метки оси Y
			Ticks: []chart.Tick{
				{Value: yValues[0], Label: fmt.Sprintf("%.6f", yValues[0])},
				{Value: yValues[1], Label: fmt.Sprintf("%.6f", yValues[1])},
				{Value: yValues[2], Label: fmt.Sprintf("%.6f", yValues[2])},
				{Value: yValues[3], Label: fmt.Sprintf("%.6f", yValues[3])},
				{Value: yValues[4], Label: fmt.Sprintf("%.6f", yValues[4])},
			},
		},
	}
	// Сохраняем график в файл
	f, _ := os.Create("equal_values.png")
	defer f.Close()
	bars.Render(chart.PNG, f)
}
func TestBabas(t *testing.T) {
	// Пример данных
	values := []chart.Value{
		{Value: 365, Label: "A"},
		{Value: 365, Label: "B"},
		{Value: 365, Label: "C"},
		{Value: 365, Label: "D"},
		{Value: 365, Label: "E"},
	}

	// Находим максимальное значение для динамического масштабирования
	maxValue := 0.0
	for _, v := range values {
		if v.Value > maxValue {
			maxValue = v.Value
		}
	}

	// Рассчитываем оптимальный шаг сетки
	gridStep := calculateGridStep(maxValue)
	maxY := math.Ceil(maxValue/gridStep) * gridStep

	// Создаем отметки для оси Y
	var ticks []chart.Tick
	for i := 0.0; i <= maxY; i += gridStep {
		ticks = append(ticks, chart.Tick{
			Value: i,
			Label: fmt.Sprintf("%.1f", i),
		})
	}

	bars := chart.BarChart{
		Title:      "Динамическая сетка",
		TitleStyle: chart.Style{Hidden: true},
		Background: chart.Style{
			Padding: chart.Box{
				Top: 40,
			},
		},
		Height:   400,
		BarWidth: 60,
		Bars:     values,
		YAxis: chart.YAxis{
			Range: &chart.ContinuousRange{
				Min: 0.0,
				Max: maxY,
			},
			Ticks: ticks,
			GridMinorStyle: chart.Style{
				StrokeColor: chart.ColorBlack,
				StrokeWidth: 0.5,
			},
			GridMajorStyle: chart.Style{
				StrokeColor:     chart.ColorBlack,
				StrokeWidth:     1.0,
				StrokeDashArray: []float64{5.0, 5.0}, // Пунктирная линия
			},
		},
	}

	f, _ := os.Create("dynamic_grid.png")
	defer f.Close()
	bars.Render(chart.PNG, f)
}

// Функция для расчета оптимального шага сетки
