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
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/dataset/api.h>
#include <arrow/dataset/plan.h>
#include <arrow/filesystem/api.h>
#include <arrow/table.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/optional.h>
#include <signal.h>
#include <iostream>
#include <memory>
#include "timer.h"

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

  return factory->Finish();
}

arrow::Status calc_mean(std::shared_ptr<ds::Dataset> dataset) {
  auto ctx = cp::default_exec_context();

  auto options = std::make_shared<ds::ScanOptions>();
  options->use_threads = true;
  ARROW_ASSIGN_OR_RAISE(
      auto project,
      ds::ProjectionDescr::FromNames({"passenger_count"}, *dataset->schema()));
  ds::SetProjection(options.get(), project);

  arrow::util::BackpressureOptions backpressure =
      arrow::util::BackpressureOptions::Make(ds::kDefaultBackpressureLow,
                                             ds::kDefaultBackpressureHigh);

  auto scan_node_options =
      ds::ScanNodeOptions{dataset, options, backpressure.toggle};

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
  ARROW_ASSIGN_OR_RAISE(auto plan, cp::ExecPlan::Make(ctx));
  ARROW_RETURN_NOT_OK(
      cp::Declaration::Sequence(
          {{"scan", scan_node_options},
           {"aggregate", cp::AggregateNodeOptions{{{"mean", nullptr}},
                                                  {"passenger_count"},
                                                  {"mean(passenger_count)"}}},
           {"sink", cp::SinkNodeOptions{&sink_gen, std::move(backpressure)}}})
          .AddToPlan(plan.get()));

  ARROW_RETURN_NOT_OK(plan->Validate());

  {
    timer t;
    ARROW_RETURN_NOT_OK(plan->StartProducing());
    auto maybe_slow_mean = sink_gen().result();
    plan->finished().Wait();

    ARROW_ASSIGN_OR_RAISE(auto slow_mean, maybe_slow_mean);
    std::cout << slow_mean->values[0].scalar()->ToString() << std::endl;
  }
  return arrow::Status::OK();
}

arrow::Status grouped_mean(std::shared_ptr<ds::Dataset> dataset) {
  auto ctx = cp::default_exec_context();

  auto options = std::make_shared<ds::ScanOptions>();
  options->use_threads = true;
  ARROW_ASSIGN_OR_RAISE(auto projection, ds::ProjectionDescr::FromNames(
                                             {"vendor_id", "passenger_count"},
                                             *dataset->schema()));
  ds::SetProjection(options.get(), projection);

  arrow::util::BackpressureOptions backpressure =
      arrow::util::BackpressureOptions::Make(ds::kDefaultBackpressureLow,
                                             ds::kDefaultBackpressureHigh);

  auto scan_node_options =
      ds::ScanNodeOptions{dataset, options, backpressure.toggle};

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
  ARROW_ASSIGN_OR_RAISE(auto plan, cp::ExecPlan::Make(ctx));
  ARROW_RETURN_NOT_OK(
      cp::Declaration::Sequence(
          {{"scan", scan_node_options},
           {"aggregate", cp::AggregateNodeOptions{{{"hash_mean", nullptr}},
                                                  {"passenger_count"},
                                                  {"mean(passenger_count)"},
                                                  {"vendor_id"}}},
           {"sink", cp::SinkNodeOptions{&sink_gen, std::move(backpressure)}}})
          .AddToPlan(plan.get()));

  auto schema =
      arrow::schema({arrow::field("mean(passenger_count)", arrow::float64()),
                     arrow::field("vendor_id", arrow::utf8())});

  std::shared_ptr<arrow::RecordBatchReader> sink_reader =
      cp::MakeGeneratorReader(schema, std::move(sink_gen), ctx->memory_pool());
  ARROW_RETURN_NOT_OK(plan->Validate());

  std::shared_ptr<arrow::Table> response_table;
  {
    timer t;
    ARROW_RETURN_NOT_OK(plan->StartProducing());
    ARROW_ASSIGN_OR_RAISE(
        response_table, arrow::Table::FromRecordBatchReader(sink_reader.get()));
  }
  std::cout << "Results: " << response_table->ToString() << std::endl;
  plan->StopProducing();
  auto future = plan->finished();
  return future.status();
}

arrow::Status grouped_filtered_mean(std::shared_ptr<ds::Dataset> dataset) {
  auto ctx = cp::default_exec_context();

  auto options = std::make_shared<ds::ScanOptions>();
  options->use_threads = true;
  options->filter = cp::greater(cp::field_ref("year"), cp::literal(2015));
  ARROW_ASSIGN_OR_RAISE(auto projection,
                        ds::ProjectionDescr::FromNames(
                            {"passenger_count", "year"}, *dataset->schema()));
  ds::SetProjection(options.get(), projection);

  arrow::util::BackpressureOptions backpressure =
      arrow::util::BackpressureOptions::Make(ds::kDefaultBackpressureLow,
                                             ds::kDefaultBackpressureHigh);

  auto scan_node_options =
      ds::ScanNodeOptions{dataset, options, backpressure.toggle};

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
  ARROW_ASSIGN_OR_RAISE(auto plan, cp::ExecPlan::Make(ctx));
  ARROW_RETURN_NOT_OK(
      cp::Declaration::Sequence(
          {{"scan", scan_node_options},
           {"filter", cp::FilterNodeOptions{cp::greater(cp::field_ref("year"),
                                                        cp::literal(2015))}},
           {"project", cp::ProjectNodeOptions{{cp::field_ref("passenger_count"),
                                               cp::field_ref("year")},
                                              {"passenger_count", "year"}}},
           {"aggregate", cp::AggregateNodeOptions{{{"hash_mean", nullptr}},
                                                  {"passenger_count"},
                                                  {"mean(passenger_count)"},
                                                  {"year"}}},
           {"sink", cp::SinkNodeOptions{&sink_gen, std::move(backpressure)}}})
          .AddToPlan(plan.get()));

  auto schema =
      arrow::schema({arrow::field("mean(passenger_count)", arrow::float64()),
                     arrow::field("year", arrow::int32())});

  std::shared_ptr<arrow::RecordBatchReader> sink_reader =
      cp::MakeGeneratorReader(schema, std::move(sink_gen), ctx->memory_pool());
  ARROW_RETURN_NOT_OK(plan->Validate());

  std::shared_ptr<arrow::Table> response_table;
  {
    timer t;
    ARROW_RETURN_NOT_OK(plan->StartProducing());
    ARROW_ASSIGN_OR_RAISE(
        response_table, arrow::Table::FromRecordBatchReader(sink_reader.get()));
  }
  std::cout << "Results: " << response_table->ToString() << std::endl;
  plan->StopProducing();
  auto future = plan->finished();
  return future.status();
}

int main(int argc, char** argv) {
  // ignore SIGPIPE errors during S3 communication
  // so we don't randomly blow up and die
  signal(SIGPIPE, SIG_IGN);

  fs::InitializeS3(fs::S3GlobalOptions{});
  auto dataset = create_dataset().ValueOrDie();

  ds::internal::Initialize();
  auto status = grouped_filtered_mean(dataset);
  if (!status.ok()) {
    std::cerr << status.message() << std::endl;
  }
}