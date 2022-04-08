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

#include <arrow/io/api.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <iostream>

int main(int argc, char** argv) {
  PARQUET_ASSIGN_OR_THROW(auto input, arrow::io::ReadableFile::Open(
                                          "../../sample_data/train.parquet"));

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  auto status = parquet::arrow::OpenFile(input, arrow::default_memory_pool(),
                                         &arrow_reader);
  if (!status.ok()) {
    std::cerr << status.message() << std::endl;
    return 1;
  }

  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(arrow_reader->ReadTable(&table));

  std::cout << table->ToString() << std::endl;

  PARQUET_ASSIGN_OR_THROW(auto outfile,
                          arrow::io::FileOutputStream::Open("train.parquet"));
  int64_t chunk_size = 1024;
  PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile, chunk_size));
  PARQUET_THROW_NOT_OK(outfile->Close());
}