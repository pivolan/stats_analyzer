package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestBarcode(t1 *testing.T) {
	a := strings.TrimRight(fmt.Sprintf("%.6f", 1.0000), "0")
	a = strings.TrimRight(a, ".")
	fmt.Println(a)
}
