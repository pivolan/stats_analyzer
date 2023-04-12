package main

import (
	"fmt"
	"testing"
)

func TestSome(t *testing.T) {
	fmt.Println(fmt.Sprintf("SELECT AVG(%[1]s), MAX(%[1]s), MIN(%[1]s), SUM(%[1]s) FROM %s", "col.Name", "tableName"))
}
