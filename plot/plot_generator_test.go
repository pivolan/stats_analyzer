package plot

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wcharczuk/go-chart/v2"
)

func TestLenPaddingBot(t *testing.T) {
	x := []string{"1", "12", "123", "1234", "12345", "123456", "1234567", "12345678", "123456789", "12345678910"}
	y := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	object := NewDataXStringsForGraph(x, y, "", "", "")
	graph, err := DrawPlotBar(object)
	assert.NoError(t, err)
	err = os.WriteFile("GraphLen.png", graph, 0655)
	assert.NoError(t, err)
}

func TestDrawPlot10(t *testing.T) {

	x := chart.RandomValues(10)
	y := chart.RandomValues(10)
	oject := NewDataDateForGraph(x, y, "count", "graphDate", "day")
	graph, err := DrawPlotBar(oject)
	assert.NoError(t, err)
	err = os.WriteFile("Graph.png", graph, 0655)
	assert.NoError(t, err)
}
func TestDrawPlot100(t *testing.T) {

	x := chart.RandomValues(100)
	y := chart.RandomValues(100)
	oject := NewDataDateForGraph(x, y, "count", "graphDate", "day")
	graph, err := DrawPlotBar(oject)
	assert.NoError(t, err)
	err = os.WriteFile("Graph1.png", graph, 0655)
	assert.NoError(t, err)
}
func TestDrawPlot500(t *testing.T) {

	x := chart.RandomValues(500)
	y := chart.RandomValues(500)
	oject := NewDataDateForGraph(x, y, "count", "graphDate", "day")
	graph, err := DrawPlotBar(oject)
	assert.NoError(t, err)
	os.WriteFile("Graph2.png", graph, 0655)
	assert.NoError(t, err)
}

func TestDrawDensityPlot(t *testing.T) {
	xValues := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	yValues := []float64{1, 2, 3, 4, 5, 6, 7, 8}

	b, err := DrawDensityPlot(xValues, yValues)
	assert.NoError(t, err)
	err = os.WriteFile("DrawDestinyPlot.png", b, 0655)
	assert.NoError(t, err)
}
