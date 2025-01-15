package main

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestComplexChart(t *testing.T) {
	a, err := DrawPlot([]float64{1, 2, 3, 4, 5, 6}, []float64{1, 2, 3, 4, 5, 6})
	assert.NoError(t, err)
	err = os.WriteFile("output.png", a, 0655)
	assert.NoError(t, err)
}
