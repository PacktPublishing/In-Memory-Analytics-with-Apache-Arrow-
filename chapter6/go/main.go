package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/PacktPublishing/In-Memory-Analytics-with-Apache-Arrow-/utils"
	"github.com/apache/arrow/go/v8/arrow/arrio"
	"github.com/apache/arrow/go/v8/arrow/flight"
	"github.com/apache/arrow/go/v8/arrow/ipc"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type server struct {
	flight.BaseFlightServer
	s3Client *s3.Client
	bucket   string
}

func NewServer() *server {
	return &server{
		s3Client: s3.New(s3.Options{Region: "us-east-2"}),
		bucket:   "ursa-labs-taxi-data",
	}
}

func (s *server) ListFlights(c *flight.Criteria, fs flight.FlightService_ListFlightsServer) error {
	var prefix string
	if len(c.Expression) > 0 {
		prefix = string(c.Expression)
	}
	list, err := s.s3Client.ListObjectsV2(fs.Context(),
		&s3.ListObjectsV2Input{
			Bucket: &s.bucket,
			Prefix: &prefix,
		})
	if err != nil {
		return err
	}

	for _, f := range list.Contents {
		if !strings.HasSuffix(*f.Key, ".parquet") {
			continue
		}
		info, err := s.getFlightInfo(fs.Context(), *f.Key, f.Size)
		if err != nil {
			return err
		}
		if err := fs.Send(info); err != nil {
			return err
		}

	}

	return nil
}

func (s *server) getFlightInfo(ctx context.Context, key string, filesize int64) (*flight.FlightInfo, error) {
	s3file, err := utils.NewS3File(ctx, s.s3Client,
		s.bucket, key, filesize)
	if err != nil {
		return nil, err
	}

	pr, err := file.NewParquetReader(s3file)
	if err != nil {
		return nil, err
	}
	defer pr.Close()

	sc, err := pqarrow.FromParquet(pr.MetaData().Schema, nil, nil)
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Schema: flight.SerializeSchema(sc, memory.DefaultAllocator),
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{key},
		},
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: []byte(key)},
		}},
		TotalRecords: pr.NumRows(),
		TotalBytes:   -1,
	}, nil
}

func (s *server) DoGet(tkt *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	path := string(tkt.Ticket)

	sf, err := utils.NewS3File(fs.Context(), s.s3Client,
		s.bucket, path, utils.UnknownSize)
	if err != nil {
		return err
	}

	pr, err := file.NewParquetReader(sf)
	if err != nil {
		return err
	}
	defer pr.Close()

	arrowRdr, err := pqarrow.NewFileReader(pr,
		pqarrow.ArrowReadProperties{
			Parallel: true, BatchSize: 100000,
		}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	rr, err := arrowRdr.GetRecordReader(fs.Context(), nil, nil)
	if err != nil {
		return err
	}
	defer rr.Release()

	wr := flight.NewRecordWriter(fs, ipc.WithSchema(rr.Schema()))
	defer wr.Close()

	n, err := arrio.Copy(wr, rr)
	fmt.Println("wrote", n, "record batches")

	return err
}

func main() {
	srv := flight.NewServerWithMiddleware(nil)
	srv.Init("0.0.0.0:0")
	srv.RegisterFlightService(NewServer())
	// the Serve function doesn’t return until the server
	// shuts down. For now we’ll start it running in a goroutine
	// and shut the server down when our main ends.
	go srv.Serve()
	defer srv.Shutdown()

	client, err := flight.NewClientWithMiddleware(srv.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err) // handle the error
	}
	defer client.Close()

	infoStream, err := client.ListFlights(context.TODO(),
		&flight.Criteria{Expression: []byte("2009")})
	if err != nil {
		panic(err) // handle the error
	}

	for {
		info, err := infoStream.Recv()
		if err != nil {
			if err == io.EOF { // we hit the end of the stream
				break
			}
			panic(err) // we got an error!
		}
		fmt.Println(info.GetFlightDescriptor().GetPath())
	}

}
