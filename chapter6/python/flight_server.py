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
from pyarrow import fs
import pyarrow.parquet as pq
import pyarrow.flight as flight

class Server(flight.FlightServerBase):
    def __init__(self, *args, **kwargs):
        # forward any arguments for now
        super().__init__(*args, **kwargs)
        self._s3 = fs.S3FileSystem(region='us-east-2', anonymous=True)
    
    def list_flights(self, context, criteria):
        path = 'ursa-labs-taxi-data'
        if len(criteria) > 0: # use criteria as start path
            path += '/' + criteria.decode('utf8')
        flist = self._s3.get_file_info(fs.FileSelector(path, recursive=True))
        for finfo in flist:
            if finfo.type == fs.FileType.Directory:
                continue
            with self._s3.open_input_file(finfo.path) as f:
                data = pq.ParquetFile(f, pre_buffer=True)
                yield flight.FlightInfo(
                    data.schema_arrow, # the arrow schema
                    flight.FlightDescriptor.for_path(f.path),
                    [flight.FlightEndpoint(f.path, [])], # no endpoints provided
                    data.metadata.num_rows,
                    -1
                )
    
    def do_get(self, context, ticket):
        input = self._s3.open_input_file(ticket.ticket.decode('utf8'))
        pf = pq.ParquetFile(input, pre_buffer=True)
        def gen():
            try:
                for batch in pf.iter_batches():
                    yield batch
            finally:
                input.close() # make sure we always close the file
        return flight.GeneratorStream(pf.schema_arrow, gen())

if __name__ == '__main__':
    with Server() as server:
        client = flight.connect(('localhost', server.port))
        for f in client.list_flights(b'2009'):
            print(f.descriptor.path, f.total_records)
        
        flights = list(client.list_flights(b'2009'))
        data = client.do_get(flights[0].endpoints[0].ticket)
        print(data.read_all())
