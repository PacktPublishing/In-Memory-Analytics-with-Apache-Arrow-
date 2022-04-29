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
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include <arrow/compute/api.h>
#include <arrow/datum.h>
#include <arrow/io/file.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <iostream>

arrow::Status compute_parquet() {
  constexpr auto filepath = "../../sample_data/yellow_tripdata_2015-01.parquet";
  ARROW_ASSIGN_OR_RAISE(auto input, arrow::io::ReadableFile::Open(filepath));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  RETURN_NOT_OK(
      parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Table> table;
  RETURN_NOT_OK(reader->ReadTable(&table));
  std::shared_ptr<arrow::ChunkedArray> column =
      table->GetColumnByName("total_amount");
  std::cout << column->ToString() << std::endl;

  ARROW_ASSIGN_OR_RAISE(
      arrow::Datum incremented,
      arrow::compute::CallFunction("add", {column, arrow::MakeScalar(5.5)}));
  // alternately we could do:
  ARROW_ASSIGN_OR_RAISE(auto other_incremented,
                        arrow::compute::Add(column, arrow::MakeScalar(5.5)));
  std::shared_ptr<arrow::ChunkedArray> output =
      std::move(incremented).chunked_array();
  std::cout << output->ToString() << std::endl;
  std::cout << other_incremented.chunked_array()->ToString() << std::endl;
  return arrow::Status::OK();
}

arrow::Status find_minmax() {
  constexpr auto filepath = "../../sample_data/yellow_tripdata_2015-01.parquet";
  ARROW_ASSIGN_OR_RAISE(auto input, arrow::io::ReadableFile::Open(filepath));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  RETURN_NOT_OK(
      parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Table> table;
  RETURN_NOT_OK(reader->ReadTable(&table));
  std::shared_ptr<arrow::ChunkedArray> column =
      table->GetColumnByName("total_amount");
  std::cout << column->ToString() << std::endl;

  arrow::compute::ScalarAggregateOptions scalar_agg_opts;
  scalar_agg_opts.skip_nulls = false;
  ARROW_ASSIGN_OR_RAISE(
      arrow::Datum minmax,
      arrow::compute::CallFunction("min_max", {column}, &scalar_agg_opts));
  std::cout << minmax.scalar_as<arrow::StructScalar>().ToString() << std::endl;
  return arrow::Status::OK();
}

arrow::Status sort_table() {
  constexpr auto filepath = "../../sample_data/yellow_tripdata_2015-01.parquet";
  ARROW_ASSIGN_OR_RAISE(auto input, arrow::io::ReadableFile::Open(filepath));
  std::unique_ptr<parquet::arrow::FileReader> reader;
  RETURN_NOT_OK(
      parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader));

  std::shared_ptr<arrow::Table> table;
  RETURN_NOT_OK(reader->ReadTable(&table));

  arrow::compute::SortOptions sort_opts;
  sort_opts.sort_keys = {arrow::compute::SortKey{
      "total_amount", arrow::compute::SortOrder::Descending}};
  ARROW_ASSIGN_OR_RAISE(
      arrow::Datum indices,
      arrow::compute::CallFunction("sort_indices", {table}, &sort_opts));

  ARROW_ASSIGN_OR_RAISE(arrow::Datum sorted,
                        arrow::compute::Take(table, indices));
  auto output = std::move(sorted).table();
  std::cout << output->ToString() << std::endl;

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  PARQUET_THROW_NOT_OK(compute_parquet());
  PARQUET_THROW_NOT_OK(find_minmax());
}