// MIT License
//
// Copyright (c) 2021 Packt
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"fmt"
	"io"
	"unsafe"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/cdata"
)

import "C"

//export processBatch
func processBatch(scptr, rbptr uintptr) {
	schema := cdata.SchemaFromPtr(scptr)
	arr := cdata.ArrayFromPtr(rbptr)

	rec, err := cdata.ImportCRecordBatch(arr, schema)
	if err != nil {
		panic(err) // handle the error!
	}
	// make sure we call the release callback when we’re done
	defer rec.Release()
	fmt.Println(rec.Schema())
	fmt.Println(rec.NumCols(), rec.NumRows())
}

//export processStream
func processStream(ptr uintptr) {
	var (
		arrstream = (*cdata.CArrowArrayStream)(unsafe.Pointer(ptr))
		rec       arrow.Record
		err       error
		x         int
	)

	rdr := cdata.ImportCArrayStream(arrstream, nil)
	for {
		rec, err = rdr.Read()
		if err != nil {
			break
		}

		fmt.Println("Batch: ", x, rec.NumCols(), rec.NumRows())
		rec.Release()
		x++
	}

	if err != io.EOF {
		panic(err) // handle the error!
	}
}

func main() {}
