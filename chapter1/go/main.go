package main

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
)

func simple_example() {
	bldr := array.NewInt64Builder(memory.DefaultAllocator)
	defer bldr.Release()
	bldr.AppendValues([]int64{1, 2, 3, 4}, nil)
	arr := bldr.NewArray()
	defer arr.Release()
	fmt.Println(arr)
}

func random_example() {
	fltBldr := array.NewFloat64Builder(memory.DefaultAllocator)
	defer fltBldr.Release()
	const ncols = 16
	columns := make([]arrow.Array, ncols)
	fields := make([]arrow.Field, ncols)
	const nrows = 8192
	for i := range columns {
		for j := 0; j < nrows; j++ {
			fltBldr.Append(rand.Float64())
		}
		columns[i] = fltBldr.NewArray()
		defer columns[i].Release()
		fields[i] = arrow.Field{
			Name: "c" + strconv.Itoa(i),
			Type: arrow.PrimitiveTypes.Float64}
	}
	record := array.NewRecord(arrow.NewSchema(fields, nil),
		columns, nrows)
	defer record.Release()
	fmt.Println(record)
}

func struct_example() {
	archerType := arrow.StructOf(
		arrow.Field{Name: "archer", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "location", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "year", Type: arrow.PrimitiveTypes.Int16},
	)

	archers := []string{"Legolas", "Oliver", "Merida", "Lara", "Artemis"}
	locations := []string{"Murkwood", "Star City", "Scotland", "London", "Greece"}
	years := []int16{1954, 1941, 2012, 1996, -600}

	mem := memory.DefaultAllocator
	strBldr := array.NewStringBuilder(mem)
	defer strBldr.Release()
	yearBldr := array.NewInt16Builder(mem)
	defer yearBldr.Release()

	strBldr.AppendValues(archers, nil)
	names := strBldr.NewArray()
	defer names.Release()
	strBldr.AppendValues(locations, nil)
	locArr := strBldr.NewArray()
	defer locArr.Release()
	yearBldr.AppendValues(years, nil)
	yearArr := yearBldr.NewArray()
	defer yearArr.Release()

	data := array.NewData(archerType, names.Len(), []*memory.Buffer{nil},
		[]arrow.ArrayData{names.Data(), locArr.Data(), yearArr.Data()}, 0, 0)
	defer data.Release()
	archerArr := array.NewStructData(data)
	defer archerArr.Release()
	fmt.Println(archerArr)
}

func struct_example_struct_builder() {
	archerList := []struct {
		archer   string
		location string
		year     int16
	}{
		{"Legolas", "Murkwood", 1954},
		{"Oliver", "Star City", 1941},
		{"Merida", "Scotland", 2012},
		{"Lara", "London", 1996},
		{"Artemis", "Greece", -600},
	}

	archerType := arrow.StructOf(
		arrow.Field{Name: "archer", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "location", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "year", Type: arrow.PrimitiveTypes.Int16},
	)

	bldr := array.NewStructBuilder(memory.DefaultAllocator, archerType)
	defer bldr.Release()
	f1b := bldr.FieldBuilder(0).(*array.StringBuilder)
	f2b := bldr.FieldBuilder(1).(*array.StringBuilder)
	f3b := bldr.FieldBuilder(2).(*array.Int16Builder)
	for _, ar := range archerList {
		bldr.Append(true)
		f1b.Append(ar.archer)
		f2b.Append(ar.location)
		f3b.Append(ar.year)
	}
	archers := bldr.NewStructArray()
	defer archers.Release()
	fmt.Println(archers)
}

func main() {
	simple_example()
	random_example()
	struct_example()
	struct_example_struct_builder()
}
