#!/usr/bin/env python3

# MIT License
#
# Copyright (c) 2021 Packt
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import pyarrow as pa
from pyarrow.cffi import ffi

# provide the function signature here
ffi.cdef("void export_int32_data(struct ArrowArray*);")
# open the shared library we compiled
lib = ffi.dlopen("../cpp/libsample.so")

# create a new pointer with ffi
c_arr = ffi.new("struct ArrowArray*")
# cast it to a uintptr_t
c_ptr = int(ffi.cast("uintptr_t", c_arr))
# call the function we made!
lib.export_int32_data(c_arr)

# import it via the C Data API so we can use it
arrnew = pa.Array._import_from_c(c_ptr, pa.int32())
# do stuff with the array like print it
print(arrnew)
del arrnew # will call the release callback once it is garbage collected
