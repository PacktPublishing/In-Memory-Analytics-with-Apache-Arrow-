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

#include <arrow/adapters/orc/adapter.h>
#include <arrow/io/api.h>
#include <arrow/table.h>
#include <iostream>

int main(int argc, char** argv) {
  // instead of explicitly handling errors, we'll just throw
  // an exception if opening the file fails by using ValueOrDie
  std::shared_ptr<arrow::io::RandomAccessFile> file =
      arrow::io::ReadableFile::Open("../../sample_data/train.orc").ValueOrDie();

  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto reader =
      arrow::adapters::orc::ORCFileReader::Open(file, pool).ValueOrDie();
  auto data = reader->Read().ValueOrDie();

  std::shared_ptr<arrow::io::OutputStream> output =
      arrow::io::FileOutputStream::Open("train.orc").ValueOrDie();
  auto writer =
      arrow::adapters::orc::ORCFileWriter::Open(output.get()).ValueOrDie();
  auto status = writer->Write(*data);
  if (!status.ok()) {
    std::cerr << status.message() << std::endl;
    return 1;
  }
  status = writer->Close();
  if (!status.ok()) {
    std::cerr << status.message() << std::endl;
    return 1;
  }
}