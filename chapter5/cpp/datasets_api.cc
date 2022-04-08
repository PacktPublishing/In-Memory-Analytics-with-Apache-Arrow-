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

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/expression.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/writer.h>
#include <iostream>
#include <memory>

#define ABORT_ON_FAIL(expr)                        \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

namespace fs = arrow::fs;
namespace ds = arrow::dataset;  // convenient
namespace cp = arrow::compute;

std::shared_ptr<arrow::Table> create_table() {
  auto schema = arrow::schema({arrow::field("a", arrow::int64()),
                               arrow::field("b", arrow::int64()),
                               arrow::field("c", arrow::int64())});
  std::shared_ptr<arrow::Array> array_a, array_b, array_c;
  arrow::NumericBuilder<arrow::Int64Type> builder;
  builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  ABORT_ON_FAIL(builder.Finish(&array_a));
  builder.Reset();
  builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0});
  ABORT_ON_FAIL(builder.Finish(&array_b));
  builder.Reset();
  builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2});
  ABORT_ON_FAIL(builder.Finish(&array_c));
  builder.Reset();
  return arrow::Table::Make(schema, {array_a, array_b, array_c});
}

std::string create_sample_dataset(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::string& root_path) {
  auto base_path = root_path + "/parquet_dataset";
  ABORT_ON_FAIL(filesystem->CreateDir(base_path));
  auto table = create_table();
  auto output =
      filesystem->OpenOutputStream(base_path + "/data1.parquet").ValueOrDie();
  ABORT_ON_FAIL(parquet::arrow::WriteTable(*table->Slice(0, 5),
                                           arrow::default_memory_pool(), output,
                                           /*chunk_size*/ 2048));
  output =
      filesystem->OpenOutputStream(base_path + "/data2.parquet").ValueOrDie();
  ABORT_ON_FAIL(parquet::arrow::WriteTable(*table->Slice(5),
                                           arrow::default_memory_pool(), output,
                                           /*chunk_size*/ 2048));
  return base_path;
}

std::shared_ptr<arrow::Table> scan_dataset(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::string& base_dir) {
  fs::FileSelector selector;
  selector.base_dir = base_dir;
  auto factory =
      ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                         ds::FileSystemFactoryOptions())
          .ValueOrDie();

  auto dataset = factory->Finish().ValueOrDie();
  auto fragments = dataset->GetFragments().ValueOrDie();
  for (const auto& fragment : fragments) {
    std::cout << "Found Fragment: " << (*fragment)->ToString() << std::endl;
  }

  auto scan_builder = dataset->NewScan().ValueOrDie();
  auto scanner = scan_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}  // end of function scan_dataset

std::shared_ptr<arrow::Table> filter_and_select(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::string& base_dir) {
  fs::FileSelector selector;
  selector.base_dir = base_dir;
  auto factory =
      ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                         ds::FileSystemFactoryOptions())
          .ValueOrDie();

  auto dataset = factory->Finish().ValueOrDie();
  auto scan_builder = dataset->NewScan().ValueOrDie();
  ABORT_ON_FAIL(scan_builder->Project({"b"}));
  ABORT_ON_FAIL(
      scan_builder->Filter(cp::less(cp::field_ref("b"), cp::literal(4))));
  auto scanner = scan_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}  // end of filter_and_select function

std::shared_ptr<arrow::Table> derive_and_rename(
    const std::shared_ptr<fs::FileSystem>& filesystem,
    const std::shared_ptr<ds::FileFormat>& format,
    const std::string& base_dir) {
  fs::FileSelector selector;
  selector.base_dir = base_dir;
  auto factory =
      ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                         ds::FileSystemFactoryOptions())
          .ValueOrDie();

  auto dataset = factory->Finish().ValueOrDie();
  auto scan_builder = dataset->NewScan().ValueOrDie();
  std::vector<std::string> names;
  std::vector<cp::Expression> exprs;
  for (const auto& field : dataset->schema()->fields()) {
    names.push_back(field->name());
    exprs.push_back(cp::field_ref(field->name()));
  }
  names.emplace_back("b_as_float32");
  exprs.push_back(cp::call("cast", {cp::field_ref("b")},
                           cp::CastOptions::Safe(arrow::float32())));

  names.emplace_back("b_large");
  // b > 1
  exprs.push_back(cp::greater(cp::field_ref("b"), cp::literal(1)));
  ABORT_ON_FAIL(scan_builder->Project(exprs, names));
  auto scanner = scan_builder->Finish().ValueOrDie();
  return scanner->ToTable().ValueOrDie();
}

int main(int argc, char** argv) {
  std::shared_ptr<fs::FileSystem> filesystem =
      std::make_shared<fs::LocalFileSystem>();
  auto path = create_sample_dataset(filesystem, "/home/zero/sample");
  std::cout << path << std::endl;

  std::shared_ptr<ds::FileFormat> format =
      std::make_shared<ds::ParquetFileFormat>();

  auto table =
      scan_dataset(filesystem, format, "/home/zero/sample/parquet_dataset");
  std::cout << table->ToString() << std::endl;
  table = filter_and_select(filesystem, format,
                            "/home/zero/sample/parquet_dataset");
  std::cout << table->ToString() << std::endl;
  table = derive_and_rename(filesystem, format,
                            "/home/zero/sample/parquet_dataset");
  std::cout << table->ToString() << std::endl;
}