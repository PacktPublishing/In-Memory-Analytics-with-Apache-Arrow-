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

#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/table.h>
#include <iostream>

arrow::Result<std::shared_ptr<arrow::Table>> read_csv(
    const std::string& filename) {
  ARROW_ASSIGN_OR_RAISE(auto input, arrow::io::ReadableFile::Open(filename));

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  ARROW_ASSIGN_OR_RAISE(
      auto reader, arrow::csv::TableReader::Make(
                       arrow::io::default_io_context(), input, read_options,
                       parse_options, convert_options));

  return reader->Read();
}

arrow::Status write_table(std::shared_ptr<arrow::Table> table,
                          const std::string& output_filename) {
  constexpr bool append = false;  // set to true to append to an existing file
  ARROW_ASSIGN_OR_RAISE(
      auto output, arrow::io::FileOutputStream::Open(output_filename, append));
  auto write_options = arrow::csv::WriteOptions::Defaults();
  return arrow::csv::WriteCSV(*table, write_options, output.get());
}

arrow::Status incremental_write(std::shared_ptr<arrow::Table> table,
                                const std::string& output_filename) {
  constexpr bool append = false;  // set to true to append to an existing file
  ARROW_ASSIGN_OR_RAISE(
      auto output, arrow::io::FileOutputStream::Open(output_filename, append));
  arrow::TableBatchReader table_reader{*table};

  auto maybe_writer = arrow::csv::MakeCSVWriter(
      output, table_reader.schema(), arrow::csv::WriteOptions::Defaults());
  if (!maybe_writer.ok()) {
    return maybe_writer.status();
  }

  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer = *maybe_writer;
  std::shared_ptr<arrow::RecordBatch> batch;
  auto status = table_reader.ReadNext(&batch);
  while (status.ok() && batch) {
    status = writer->WriteRecordBatch(*batch);
    if (!status.ok()) {
      return status;
    }
    status = table_reader.ReadNext(&batch);
  }

  if (!status.ok()) {
    return status;
  }

  RETURN_NOT_OK(writer->Close());
  RETURN_NOT_OK(output->Close());

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  std::shared_ptr<arrow::Table> table =
      read_csv("../../sample_data/train.csv").ValueOrDie();

  auto status = write_table(table, "train.csv");
  if (!status.ok()) {
    std::cerr << status.message() << std::endl;
    return 1;
  }

  status = incremental_write(table, "train.csv");
  if (!status.ok()) {
    std::cerr << status.message() << std::endl;
    return 1;
  }
}