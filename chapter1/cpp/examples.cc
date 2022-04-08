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
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <iostream>
#include <memory>
#include <random>
#include <vector>

void first_example() {
  std::vector<int64_t> data{1, 2, 3, 4};
  auto arr = std::make_shared<arrow::Int64Array>(data.size(),
                                                 arrow::Buffer::Wrap(data));
  std::cout << arr->ToString() << std::endl;
}

void random_data_example() {
  std::random_device rd;
  std::mt19937 gen{rd()};
  std::normal_distribution<> d{5, 2};

  auto pool = arrow::default_memory_pool();
  arrow::DoubleBuilder builder{arrow::float64(), pool};

  constexpr auto ncols = 16;
  constexpr auto nrows = 8192;
  arrow::ArrayVector columns(ncols);
  arrow::FieldVector fields;
  for (int i = 0; i < ncols; ++i) {
    for (int j = 0; j < nrows; ++j) {
      builder.Append(d(gen));
    }
    auto status = builder.Finish(&columns[i]);
    if (!status.ok()) {
      std::cerr << status.message() << std::endl;
      // do something!
      return;
    }
    fields.push_back(arrow::field("c" + std::to_string(i), arrow::float64()));
  }

  auto rb = arrow::RecordBatch::Make(arrow::schema(fields),
                                     columns[0]->length(), columns);
  std::cout << rb->ToString() << std::endl;
}

void building_struct_array() {
  using arrow::field;
  using arrow::int16;
  using arrow::utf8;
  arrow::ArrayVector children;

  std::vector<std::string> archers{"Legolas", "Oliver", "Merida", "Lara",
                                   "Artemis"};
  std::vector<std::string> locations{"Murkwood", "Star City", "Scotland",
                                     "London", "Greece"};
  std::vector<int16_t> years{1954, 1941, 2012, 1996, -600};

  children.resize(3);
  arrow::StringBuilder str_bldr;
  str_bldr.AppendValues(archers);
  str_bldr.Finish(&children[0]);
  str_bldr.AppendValues(locations);
  str_bldr.Finish(&children[1]);
  arrow::Int16Builder year_bldr;
  year_bldr.AppendValues(years);
  year_bldr.Finish(&children[2]);

  arrow::StructArray arr{
      arrow::struct_({field("archer", utf8()), field("location", utf8()),
                      field("year", int16())}),
      children[0]->length(), children};
  std::cout << arr.ToString() << std::endl;
}

void build_struct_builder() {
  using arrow::field;
  std::shared_ptr<arrow::DataType> st_type = arrow::struct_(
      {field("archer", arrow::utf8()), field("location", arrow::utf8()),
       field("year", arrow::int16())});

  std::unique_ptr<arrow::ArrayBuilder> tmp;
  arrow::MakeBuilder(arrow::default_memory_pool(), st_type, &tmp);
  std::shared_ptr<arrow::StructBuilder> builder;
  builder.reset(static_cast<arrow::StructBuilder*>(tmp.release()));

  using namespace arrow;
  StringBuilder* archer_builder =
      static_cast<StringBuilder*>(builder->field_builder(0));
  StringBuilder* location_builder =
      static_cast<StringBuilder*>(builder->field_builder(1));
  Int16Builder* year_builder =
      static_cast<Int16Builder*>(builder->field_builder(2));

  std::vector<std::string> archers{"Legolas", "Oliver", "Merida", "Lara",
                                   "Artemis"};
  std::vector<std::string> locations{"Murkwood", "Star City", "Scotland",
                                     "London", "Greece"};
  std::vector<int16_t> years{1954, 1941, 2012, 1996, -600};

  for (int i = 0; i < archers.size(); ++i) {
    builder->Append();
    archer_builder->Append(archers[i]);
    location_builder->Append(locations[i]);
    year_builder->Append(years[i]);
  }

  std::shared_ptr<arrow::Array> out;
  builder->Finish(&out);
  std::cout << out->ToString() << std::endl;
}

int main(int argc, char** argv) {
  first_example();
  random_data_example();
  building_struct_array();
  build_struct_builder();
}