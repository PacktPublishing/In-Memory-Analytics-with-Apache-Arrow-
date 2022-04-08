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

#include <arrow/csv/api.h>  // the csv functions and objects
#include <arrow/io/api.h>   // for opening the file
#include <arrow/table.h>    // to read the data into a table
#include <iostream>         // to output to the terminal

int main(int argc, char** argv) {
  auto maybe_input =
      arrow::io::ReadableFile::Open("../../sample_data/train.csv");
  if (!maybe_input.ok()) {
    // handle any file open errors
    std::cerr << maybe_input.status().message() << std::endl;
    return 1;
  }

  std::shared_ptr<arrow::io::InputStream> input = *maybe_input;
  auto io_context = arrow::io::default_io_context();
  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  auto maybe_reader = arrow::csv::TableReader::Make(
      io_context, input, read_options, parse_options, convert_options);

  if (!maybe_reader.ok()) {
    // handle any instantiation errors
    std::cerr << maybe_reader.status().message() << std::endl;
    return 1;
  }

  std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;
  // finally read the data from the file
  auto maybe_table = reader->Read();
  if (!maybe_table.ok()) {
    // handle any errors such as CSV syntax errors
    // or failed type conversion errors, etc.
    std::cerr << maybe_table.status().message() << std::endl;
    return 1;
  }

  std::shared_ptr<arrow::Table> table = *maybe_table;
  std::cout << table->ToString() << std::endl;
}