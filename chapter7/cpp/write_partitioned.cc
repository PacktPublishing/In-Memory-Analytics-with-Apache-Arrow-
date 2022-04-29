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
#include <iostream>

namespace fs = arrow::fs;
namespace ds = arrow::dataset;
namespace cp = arrow::compute;

arrow::Result<std::shared_ptr<ds::Dataset>> create_dataset() {
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
  ARROW_ASSIGN_OR_RAISE(auto factory,
                        ds::FileSystemDatasetFactory::Make(filesystem, selector,
                                                           format, options));
  ds::FinishOptions finopts;
  finopts.validate_fragments = true;
  finopts.inspect_options.fragments = ds::InspectOptions::kInspectAllFragments;
  return factory->Finish(finopts);
}

arrow::Status write_dataset(std::shared_ptr<ds::Dataset> dataset) {
  auto scan_builder = dataset->NewScan().ValueOrDie();
  scan_builder->UseThreads(true);
  scan_builder->BatchSize(1 << 28);
  scan_builder->Filter(cp::and_({
      cp::greater_equal(cp::field_ref("year"), cp::literal(2014)),
      cp::less_equal(cp::field_ref("year"), cp::literal(2015)),
  }));
  auto scanner = scan_builder->Finish().ValueOrDie();
  std::cout << dataset->schema()->ToString() << std::endl;

  std::shared_ptr<fs::FileSystem> filesystem =
      std::make_shared<fs::LocalFileSystem>();

  auto base_path = "/home/zero/sample/csv_dataset";
  auto format = std::make_shared<ds::CsvFileFormat>();
  ds::FileSystemDatasetWriteOptions write_opts;
  auto csv_write_options = std::static_pointer_cast<ds::CsvFileWriteOptions>(
      format->DefaultWriteOptions());
  csv_write_options->write_options->delimiter = '|';
  write_opts.file_write_options = csv_write_options;
  write_opts.filesystem = filesystem;
  write_opts.base_dir = base_path;
  write_opts.partitioning = std::make_shared<ds::HivePartitioning>(
      arrow::schema({arrow::field("year", arrow::int32()),
                     arrow::field("month", arrow::int32())}));

  write_opts.basename_template = "part{i}.csv";
  return ds::FileSystemDataset::Write(write_opts, scanner);
}

int main(int argc, char** argv) {
  // ignore SIGPIPE errors during S3 communication
  // so we don't randomly blow up and die
  signal(SIGPIPE, SIG_IGN);

  fs::InitializeS3(fs::S3GlobalOptions{});
  auto dataset = create_dataset().ValueOrDie();
  auto status = write_dataset(dataset);
  if (!status.ok()) {
    std::cerr << status.message() << std::endl;
    return 1;
  }
}