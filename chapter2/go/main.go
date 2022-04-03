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
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/csv"
	"github.com/apache/arrow/go/v7/arrow/ipc"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	client := s3.New(s3.Options{Region: "us-east-1"})
	obj, err := client.GetObject(context.Background(),
		&s3.GetObjectInput{
			Bucket: aws.String("nyc-tlc"),
			Key:    aws.String("trip data/yellow_tripdata_2020-11.csv"),
		})
	if err != nil {
		panic(err)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "VendorID",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name:     "tpep_pickup_datetime",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		},
		{
			Name:     "tpep_dropoff_datetime",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		},
		{
			Name:     "passenger_count",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name:     "trip_distance",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name:     "RatecodeID",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name:     "store_and_fwd_flag",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		},
		{
			Name:     "PULocationID",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name:     "DOLocationID",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name:     "payment_type",
			Type:     arrow.BinaryTypes.String,
			Nullable: true,
		},
		{
			Name:     "fare_amount",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name:     "extra",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name:     "mta_tax",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name:     "tip_amount",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name:     "tolls_amount",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name:     "improvement_surcharge",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name:     "total_amount",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name:     "congestion_surcharge",
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
	}, nil)

	reader := csv.NewReader(obj.Body, schema, csv.WithHeader(true))
	defer reader.Release()

	w, _ := os.Create("tripdata.arrow")
	writer := ipc.NewWriter(w, ipc.WithSchema(reader.Schema()))
	defer writer.Close()

	for reader.Next() {
		if err := writer.Write(reader.Record()); err != nil {
			break
		}
	}
	if reader.Err() != nil {
		fmt.Println(reader.Err())
	}
}
