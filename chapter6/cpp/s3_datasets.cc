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
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <atomic>
#include <iostream>
#include <memory>
#include "timer.h"

#define ABORT_ON_FAIL(expr)                        \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

namespace fs = arrow::fs;
namespace ds = arrow::dataset;
namespace cp = arrow::compute;

void timing_test() {
  auto opts = fs::S3Options::Anonymous();
  opts.region = "us-east-2";

  std::shared_ptr<ds::FileFormat> format =
      std::make_shared<ds::ParquetFileFormat>();
  std::shared_ptr<fs::FileSystem> filesystem =
      fs::S3FileSystem::Make(opts).ValueOrDie();
  fs::FileSelector selector;
  selector.base_dir = "ursa-labs-taxi-data";
  selector.recursive = true;  // check all the subdirectories

  std::shared_ptr<ds::DatasetFactory> factory;
  std::shared_ptr<ds::Dataset> dataset;

  {
    timer t;
    factory = ds::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                                 ds::FileSystemFactoryOptions())
                  .ValueOrDie();
    dataset = factory->Finish().ValueOrDie();
  }

  auto scan_builder = dataset->NewScan().ValueOrDie();
  auto scanner = scan_builder->Finish().ValueOrDie();

  {
    timer t;
    std::cout << scanner->CountRows().ValueOrDie() << std::endl;
  }
}

void compute_mean() {
  auto opts = fs::S3Options::Anonymous();
  opts.region = "us-east-2";

  std::shared_ptr<ds::FileFormat> format =
      std::make_shared<ds::ParquetFileFormat>();
  std::shared_ptr<fs::FileSystem> filesystem =
      fs::S3FileSystem::Make(opts).ValueOrDie();
  fs::FileSelector selector;
  selector.base_dir = "ursa-labs-taxi-data";
  selector.recursive = true;  // check all the subdirectories

  std::shared_ptr<ds::DatasetFactory> factory;
  std::shared_ptr<ds::Dataset> dataset;

  {
    timer t;
    auto scan_builder = dataset->NewScan().ValueOrDie();
    scan_builder->BatchSize(1 << 28);  // default is 1 << 20
    scan_builder->UseThreads(true);
    scan_builder->Project({"passenger_count"});
    auto scanner = scan_builder->Finish().ValueOrDie();
    std::atomic<int64_t> passengers(0), count(0);
    ABORT_ON_FAIL(
        scanner->Scan([&](ds::TaggedRecordBatch batch) -> arrow::Status {
          ARROW_ASSIGN_OR_RAISE(
              auto result,
              cp::Sum(batch.record_batch->GetColumnByName("passenger_count")));
          passengers += result.scalar_as<arrow::Int64Scalar>().value;
          count += batch.record_batch->num_rows();
          return arrow::Status::OK();
        }));
    double mean = double(passengers.load()) / double(count.load());
    std::cout << mean << std::endl;
  }  // end of the timer block
}

void scan_fragments() {
  auto opts = fs::S3Options::Anonymous();
  opts.region = "us-east-2";

  std::shared_ptr<ds::FileFormat> format =
      std::make_shared<ds::ParquetFileFormat>();
  std::shared_ptr<fs::FileSystem> filesystem =
      fs::S3FileSystem::Make(opts).ValueOrDie();
  fs::FileSelector selector;
  selector.base_dir = "ursa-labs-taxi-data";
  selector.recursive = true;  // check all the subdirectories

  ds::FileSystemFactoryOptions options;
  options.partitioning =
      ds::DirectoryPartitioning::MakeFactory({"year", "month"});

  auto factory =
      ds::FileSystemDatasetFactory::Make(filesystem, selector, format, options)
          .ValueOrDie();
  auto dataset = factory->Finish().ValueOrDie();
  auto fragments = dataset->GetFragments().ValueOrDie();

  for (const auto& fragment : fragments) {
    std::cout << "Found Fragment: " << (*fragment)->ToString() << std::endl;
    std::cout << "Partition Expression: "
              << (*fragment)->partition_expression().ToString() << std::endl;
  }

  std::cout << dataset->schema()->ToString() << std::endl;
}

int main(int argc, char** argv) {
  fs::InitializeS3(fs::S3GlobalOptions{});
  timing_test();
  compute_mean();
  scan_fragments();
}