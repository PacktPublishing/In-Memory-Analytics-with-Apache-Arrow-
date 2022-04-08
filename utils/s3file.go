package utils

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3file struct {
	Context context.Context
	Client  *s3.Client
	bucket  string
	key     string

	pos, length int64
}

const UnknownSize int64 = -1

func NewS3File(ctx context.Context, client *s3.Client, bucket, key string, length int64) (*s3file, error) {
	f := &s3file{
		Context: ctx,
		Client:  client,
		bucket:  bucket,
		key:     key,
		length:  length,
	}

	if f.length == UnknownSize {
		out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		if err != nil {
			return nil, err
		}

		f.length = out.ContentLength
	}
	return f, nil
}

func (sf *s3file) Read(p []byte) (n int, err error) {
	return sf.ReadAt(p, sf.pos)
}

func (sf *s3file) Seek(offset int64, whence int) (int64, error) {
	newPos, offs := int64(0), offset
	switch whence {
	case io.SeekStart:
		newPos = offs
	case io.SeekCurrent:
		newPos = sf.pos + offs
	case io.SeekEnd:
		newPos = int64(sf.length) + offs
	}

	if newPos < 0 {
		return 0, errors.New("negative result pos")
	}
	if newPos > int64(sf.length) {
		return 0, errors.New("new position exceeds size of file")
	}
	sf.pos = newPos
	return newPos, nil
}

func (sf *s3file) ReadAt(p []byte, off int64) (n int, err error) {
	end := off + int64(len(p)) - 1
	if end >= sf.length {
		end = sf.length - 1
	}

	out, err := sf.Client.GetObject(sf.Context, &s3.GetObjectInput{
		Bucket: &sf.bucket,
		Key:    &sf.key,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, end)),
	})
	if err != nil {
		return 0, err
	}

	return io.ReadAtLeast(out.Body, p, int(out.ContentLength))
}
