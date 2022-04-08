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
import pyarrow.csv
import pyarrow.parquet as pq
import pandas as pd

# run this from the sample_data directory to generate the .parquet
# .arrow and -nonan.arrow files

tbl = pa.csv.read_csv('yellow_tripdata_2015-01.csv')
pq.write_table(tbl, 'yellow_tripdata_2015-01.parquet')
with pa.OSFile('yellow_tripdata_2015-01.arrow', 'wb') as sink:
    with pa.RecordBatchFileWriter(sink, tbl.schema) as writer:
        writer.write_table(tbl)

# fill out the NaN values with 0s so that we can zero-copy it to
# a pandas dataframe in our memory usage test
df = tbl.to_pandas().fillna(0)
tbl = pa.Table.from_pandas(df)
with pa.OSFile('yellow_tripdata_2015-01-nonan.arrow', 'wb') as sink:
    with pa.RecordBatchFileWriter(sink, tbl.schema) as writer:
        writer.write_table(tbl)
