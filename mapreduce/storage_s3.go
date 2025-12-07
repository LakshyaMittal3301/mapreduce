package mr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

type S3Storage struct {
	bucket string
	client *s3.Client

	jobId              string
	inputPrefix        string
	intermediatePrefix string
	outputPrefix       string
}

const maxS3Concurrency = 16

func NewS3Storage(bucket, inputPrefix string) (Storage, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	if inputPrefix != "" && !strings.HasSuffix(inputPrefix, "/") {
		inputPrefix += "/"
	}
	return &S3Storage{
		bucket:      bucket,
		client:      client,
		inputPrefix: inputPrefix,
	}, nil
}

func (s *S3Storage) SetJob(jobId string) {
	s.jobId = jobId
	s.intermediatePrefix = fmt.Sprintf("jobs/%s/intermediate/", jobId)
	s.outputPrefix = fmt.Sprintf("jobs/%s/output/", jobId)
}

func (s *S3Storage) ReadInput(filename string) (string, error) {
	key := s.inputPrefix + filename

	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
	})
	if err != nil {
		return "", fmt.Errorf("get S3 object %s: %w", key, err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return "", fmt.Errorf("read S3 object %s: %w", key, err)
	}
	Infof("S3Storage: read input from s3://%s/%s (%d bytes)", s.bucket, key, len(data))
	return string(data), nil
}

func acquire(ctx context.Context, sem chan struct{}) error {
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func release(sem chan struct{}) {
	select {
	case <-sem:
	default:
	}
}

func (s *S3Storage) WriteIntermediate(mapID int, nReduce int, buckets [][]KeyValue) error {
	sem := make(chan struct{}, maxS3Concurrency)
	g, ctx := errgroup.WithContext(context.Background())

	for r := 0; r < nReduce; r++ {
		r := r
		g.Go(func() error {
			if err := acquire(ctx, sem); err != nil {
				return err
			}
			defer release(sem)

			key := fmt.Sprintf("%smr-%d-%d.txt", s.intermediatePrefix, mapID, r)

			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range buckets[r] {
				if err := enc.Encode(&kv); err != nil {
					return fmt.Errorf("encode intermediate for map=%d reduce=%d: %w", mapID, r, err)
				}
			}

			_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      &s.bucket,
				Key:         &key,
				Body:        bytes.NewReader(buf.Bytes()),
				ContentType: aws.String("text/plain"),
			})
			if err != nil {
				return fmt.Errorf("put S3 object %s: %w", key, err)
			}
			Infof("S3Storage: wrote intermediate to s3://%s/%s (map=%d reduce=%d size=%d)", s.bucket, key, mapID, r, buf.Len())
			return nil
		})
	}

	return g.Wait()
}

func (s *S3Storage) ReadIntermediateForReduce(reduceID int, nMap int) ([]KeyValue, error) {
	sem := make(chan struct{}, maxS3Concurrency)
	g, ctx := errgroup.WithContext(context.Background())
	perMap := make([][]KeyValue, nMap)

	for m := 0; m < nMap; m++ {
		m := m
		g.Go(func() error {
			if err := acquire(ctx, sem); err != nil {
				return err
			}
			defer release(sem)
			key := fmt.Sprintf("%smr-%d-%d.txt", s.intermediatePrefix, m, reduceID)

			out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &s.bucket,
				Key:    &key,
			})
			if err != nil {
				// For now, simplest behavior: if object not found, skip.
				// We'll treat any "no such key" as "this map produced no data for this reduce".
				var nsk *s3types.NoSuchKey
				if errors.As(err, &nsk) {
					return nil
				}
				return fmt.Errorf("get S3 object %s: %w", key, err)
			}
			defer out.Body.Close()

			dec := json.NewDecoder(out.Body)
			var local []KeyValue
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("decode intermediate %s: %w", key, err)
				}
				local = append(local, kv)
			}
			perMap[m] = local
			Debugf("S3Storage: read intermediate from s3://%s/%s (%d kvs)", s.bucket, key, len(local))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var result []KeyValue
	for _, kvs := range perMap {
		result = append(result, kvs...)
	}

	return result, nil
}

func (s *S3Storage) WriteOutput(reduceID int, kvs []KeyValue) error {
	var buf bytes.Buffer
	for _, kv := range kvs {
		if _, err := fmt.Fprintf(&buf, "%v %v\n", kv.Key, kv.Value); err != nil {
			return fmt.Errorf("format output: %w", err)
		}
	}

	key := fmt.Sprintf("%smr-out-%d.txt", s.outputPrefix, reduceID)

	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      &s.bucket,
		Key:         &key,
		Body:        bytes.NewReader(buf.Bytes()),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		return fmt.Errorf("put S3 object %s: %w", key, err)
	}

	Infof("S3Storage: wrote output to s3://%s/%s (%d records, %d bytes)", s.bucket, key, len(kvs), buf.Len())
	return nil
}
