package mr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3Storage struct {
	bucket string
	client *s3.Client

	jobId              string
	intermediatePrefix string
	outputPrefix       string
}

func NewS3Storage(bucket string) (Storage, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3Storage{
		bucket: bucket,
		client: client,
	}, nil
}

func (s *S3Storage) SetJob(jobId string) {
	s.jobId = jobId
	s.intermediatePrefix = fmt.Sprintf("jobs/%s/intermediate/", jobId)
	s.outputPrefix = fmt.Sprintf("jobs/%s/output/", jobId)
}

func (s *S3Storage) ReadInput(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", fmt.Errorf("cannot open %v, err: %v", filename, err)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		return "", fmt.Errorf("cannot read %v, err: %v", filename, err)
	}
	return string(content), nil
}

func (s *S3Storage) WriteIntermediate(mapID int, nReduce int, buckets [][]KeyValue) error {
	for r := 0; r < nReduce; r++ {
		key := fmt.Sprintf("%smr-%d-%d", s.intermediatePrefix, mapID, r)

		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		for _, kv := range buckets[r] {
			if err := enc.Encode(&kv); err != nil {
				return fmt.Errorf("encode intermediate for map=%d reduce=%d: %w", mapID, r, err)
			}
		}

		_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
			Body:   bytes.NewReader(buf.Bytes()),
		})
		if err != nil {
			return fmt.Errorf("put S3 object %s: %w", key, err)
		}
	}
	return nil
}

func (s *S3Storage) ReadIntermediateForReduce(reduceID int, nMap int) ([]KeyValue, error) {
	var result []KeyValue

	for m := 0; m < nMap; m++ {
		key := fmt.Sprintf("%smr-%d-%d", s.intermediatePrefix, m, reduceID)

		out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
		})
		if err != nil {
			// For now, simplest behavior: if object not found, skip.
			// We'll treat any "no such key" as "this map produced no data for this reduce".
			var nsk *s3types.NoSuchKey
			if errors.As(err, &nsk) {
				continue
			}
			return nil, fmt.Errorf("get S3 object %s: %w", key, err)
		}

		dec := json.NewDecoder(out.Body)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				out.Body.Close()
				return nil, fmt.Errorf("decode intermediate %s: %w", key, err)
			}
			result = append(result, kv)
		}
		out.Body.Close()
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

	key := fmt.Sprintf("%smr-out-%d", s.outputPrefix, reduceID)

	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &key,
		Body:   bytes.NewReader(buf.Bytes()),
	})
	if err != nil {
		return fmt.Errorf("put S3 object %s: %w", key, err)
	}

	return nil
}
