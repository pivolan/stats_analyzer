package main

import (
	"fmt"
	"reflect"
	"testing"
)

func TestBarcode(t1 *testing.T) {
	t := reflect.TypeOf(CommonStat{})

	// Loop over the fields in the struct and print their names
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fmt.Println(field.Name)
	}
}
