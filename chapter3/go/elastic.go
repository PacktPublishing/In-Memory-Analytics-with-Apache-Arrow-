package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
)

type property struct {
	Type   string      `json:"type"`
	Format string      `json:"format,omitempty"`
	Fields interface{} `json:"fields,omitempty"`
}

var keywordField = &struct {
	Keyword interface{} `json:"keyword"`
}{struct {
	Type        string `json:"type"`
	IgnoreAbove int    `json:"ignore_above"`
}{"keyword", 256}}

var primitiveMapping = map[arrow.Type]string{
	arrow.BOOL:       "boolean",
	arrow.INT8:       "byte",
	arrow.UINT8:      "short", // no unsigned byte type
	arrow.INT16:      "short",
	arrow.UINT16:     "integer", // no unsigned short
	arrow.INT32:      "integer",
	arrow.UINT32:     "long", // no unsigned integer
	arrow.INT64:      "long",
	arrow.UINT64:     "unsigned_long",
	arrow.FLOAT16:    "half_float",
	arrow.FLOAT32:    "float",
	arrow.FLOAT64:    "double",
	arrow.DECIMAL:    "scaled_float",
	arrow.DECIMAL256: "scaled_float",
	arrow.BINARY:     "binary",
	arrow.STRING:     "text",
}

func createMapping(sc *arrow.Schema) map[string]property {
	mappings := make(map[string]property)
	for _, f := range sc.Fields() {
		var (
			p  property
			ok bool
		)
		if p.Type, ok = primitiveMapping[f.Type.ID()]; !ok {
			switch f.Type.ID() {
			case arrow.DATE32, arrow.DATE64:
				p.Type = "date"
				p.Format = "yyyy-MM-dd"
			case arrow.TIME32, arrow.TIME64:
				p.Type = "date"
				p.Format = "time||time_no_millis"
			case arrow.TIMESTAMP:
				p.Type = "date"
				p.Format = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd HH:mm:ss.SSSSSSSSS"
			case arrow.STRING:
				p.Type = "text" // or keyword
				p.Fields = keywordField
			}
		}
		mappings[f.Name] = p
	}
	return mappings
}

func main() {
	// the second argument is a bool value for memory mapping
	// the file if desired.
	parq, err := file.OpenParquetFile("../../sample_data/sliced.parquet", false)
	if err != nil {
		// handle the error
		panic(err)
	}
	defer parq.Close()

	props := pqarrow.ArrowReadProperties{BatchSize: 50000}
	rdr, err := pqarrow.NewFileReader(parq, props,
		memory.DefaultAllocator)
	if err != nil {
		// handle error
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// leave these empty since we're not filtering out any
	// columns or row groups. But if you wanted to do so,
	// this is how you'd optimize the read
	var cols, rowgroups []int
	rr, err := rdr.GetRecordReader(ctx, cols, rowgroups)
	if err != nil {
		// handle the error
		panic(err)
	}

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		// handle the error
		panic(err)
	}

	var mapping struct {
		Mappings struct {
			Properties map[string]property `json:"properties"`
		} `json:"mappings"`
	}
	mapping.Mappings.Properties = createMapping(rr.Schema())
	response, err := es.Indices.Create("indexname",
		es.Indices.Create.WithBody(
			esutil.NewJSONReader(mapping)))
	if err != nil {
		// handle error
		panic(err)
	}
	if response.StatusCode != http.StatusOK {
		// handle failure response and return/exit
		panic(fmt.Errorf("non-ok status: %s", response.Status()))
	}
	// Index created!

	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: es, Index: "indexname",
		OnError: func(_ context.Context, err error) {
			fmt.Println(err)
		},
	})
	if err != nil {
		// handle error
		panic(err)
	}

	pr, pw := io.Pipe() // to pass the data
	go func() {
		for rr.Next() {
			if err := array.RecordToJSON(rr.Record(), pw); err != nil {
				cancel()
				pw.CloseWithError(err)
				return
			}
		}
		pw.Close()
	}()

	scanner := bufio.NewScanner(pr)
	for scanner.Scan() {
		err = indexer.Add(ctx, esutil.BulkIndexerItem{
			Action: "index",
			Body:   strings.NewReader(scanner.Text()),
			OnFailure: func(_ context.Context,
				item esutil.BulkIndexerItem,
				resp esutil.BulkIndexerResponseItem,
				err error) {
				fmt.Printf("Failure! %s, %+v\n%+v\n", err, item, resp)
			},
		})
		if err != nil {
			// handle the error
			panic(err)
		}
	}

	if err = indexer.Close(ctx); err != nil {
		// handle error
		panic(err)
	}

}
