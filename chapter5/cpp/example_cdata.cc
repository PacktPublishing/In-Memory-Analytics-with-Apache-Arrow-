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

#include <algorithm>
#include <limits>
#include <random>
#include <vector>
#include <arrow/c/abi.h>

std::vector<int32_t> generate_data(size_t size) {
    static std::uniform_int_distribution<int32_t> dist(
        std::numeric_limits<int32_t>::min(),
        std::numeric_limits<int32_t>::max());
    static std::random_device rnd_device;
    std::default_random_engine generator(rnd_device());
    std::vector<int32_t> data(size);
    std::generate(data.begin(), data.end(), [&]() {
        return dist(generator); });
    return data;
}

extern "C" {
    void export_int32_data(struct ArrowArray*);
}

void export_int32_data(struct ArrowArray* array) {
    std::vector<int32_t>* vecptr = 
        new std::vector<int32_t>(std::move(generate_data(1000)));

    *array = (struct ArrowArray) {
        .length = vecptr->size(),
        .null_count = 0,
        .offset = 0,
        .n_buffers = 2,
        .n_children = 0,
        .buffers = (const void**)malloc(sizeof(void*)*2),
        .children = nullptr,
        .dictionary = nullptr,
        .release = [](struct ArrowArray* arr) {
            free(arr->buffers);
            delete reinterpret_cast<
               std::vector<int32_t>*>(
                 arr->private_data);
            arr->release = nullptr;
        },
        .private_data = reinterpret_cast<void*>(vecptr),
    };
    array->buffers[0] = nullptr;
    array->buffers[1] = vecptr->data();
} // end of function
