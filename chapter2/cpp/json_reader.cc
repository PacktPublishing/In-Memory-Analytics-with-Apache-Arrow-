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

#include <arrow/json/api.h>
#include <arrow/io/api.h>
#include <arrow/table.h>
#include <iostream>

int main(int argc, char** argv) {
    auto read_options = arrow::json::ReadOptions::Defaults();
    auto parse_options = arrow::json::ParseOptions::Defaults();

    constexpr auto filename = "sample.json"; // could be any json file

    auto maybe_input = arrow::io::ReadableFile::Open(filename);
    if (!maybe_input.ok()) {
        std::cerr << maybe_input.status().message() << std::endl;
        return 1;
    }

    std::shared_ptr<arrow::io::InputStream> input = *maybe_input;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto maybe_reader = arrow::json::TableReader::Make(pool, input, read_options, parse_options);
    if (!maybe_reader.ok()) {
        std::cerr << maybe_reader.status().message() << std::endl;
        return 1;
    }
    std::shared_ptr<arrow::json::TableReader> reader = *maybe_reader;
    auto maybe_table = reader->Read();
    if (!maybe_table.ok()) {
        std::cerr << maybe_table.status().message() << std::endl;
        return 1;
    }

    std::shared_ptr<arrow::Table> table = *maybe_table;
    std::cout << table->ToString() << std::endl;
}