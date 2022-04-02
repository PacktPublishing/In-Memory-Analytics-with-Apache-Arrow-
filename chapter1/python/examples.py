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

# random record batch generator

import pyarrow as pa
import numpy as np
NROWS = 8192
NCOLS = 16
data = [pa.array(np.random.randn(NROWS)) for i in range(NCOLS)]
cols = ['c' + str(i) for i in range(NCOLS)] 
rb = pa.RecordBatch.from_arrays(data, cols)
print(rb.schema)
print(rb.num_rows)

# building a struct array

archer_list = [{
    'archer': 'Legolas',
    'location': 'Murkwood',
    'year': 1954,
},{
    'archer': 'Oliver',
    'location': 'Star City',
    'year': 1941,
},{
    'archer': 'Merida',
    'location': 'Scotland',
    'year': 2012,
},{
    'archer': 'Lara',
    'location': 'London',
    'year': 1996,
},{
    'archer': 'Artemis',
    'location': 'Greece',
    'year': -600,
}]

archer_type = pa.struct([('archer', pa.utf8()),
                         ('location', pa.utf8()),
                         ('year', pa.int16())])
archers = pa.array(archer_list, type=archer_type)
print(archers.type)
print(archers)

rb = pa.RecordBatch.from_arrays(archers.flatten(), 
                                ['archer', 'location', 'year'])
print(rb)
print(rb.num_rows) # prints 5
print(rb.num_columns) # prints 3

# slices

slice = rb.slice(1, 3) # (start, length)
print(slice.num_rows) # prints 3, not 5
print(rb.column(0)[0]) # <pyarrow.StringScalar: 'Legolas'>
print(slice.column(0)[0]) # <pyarrow.StringScalar: 'Oliver'>
archerslice = archers[1:3] # slice of length 2 viewing indexes 
                           # 1 and 2 from the struct array
                           # so it slices all three arrays
print(rb.to_pydict()) # prints dict {column: list<values>}
print(archers.to_pylist()) # prints the same list of dictionaries
                           # we started with