package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Storage interface {
	SetJob(jobId string)

	// Read / Write for Map tasks
	ReadInput(filename string) (string, error)
	WriteIntermediate(mapID int, nReduce int, kva [][]KeyValue) error

	// Read / Write for Reduce tasks
	ReadIntermediateForReduce(reduceId int, nMap int) ([]KeyValue, error)
	WriteOutput(reduceID int, kvs []KeyValue) error
}

type LocalStorage struct {
	jobId string
}

func NewLocalStorage() Storage {
	return &LocalStorage{}
}

func (ls *LocalStorage) SetJob(jobId string) {
	ls.jobId = jobId
}

func (ls *LocalStorage) ReadInput(filename string) (string, error) {
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

func (ls *LocalStorage) WriteIntermediate(mapId int, nReduce int, kva [][]KeyValue) error {
	for r := range nReduce {
		finalName := fmt.Sprintf("job/%s/intermediate/mr-%d-%d", ls.jobId, mapId, r)
		dir := filepath.Dir(finalName)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}

		tmpFile, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			return err
		}

		enc := json.NewEncoder(tmpFile)

		for _, kv := range kva[r] {
			if err := enc.Encode(&kv); err != nil {
				tmpFile.Close()
				_ = os.Remove(tmpFile.Name())
				return err
			}
		}

		if err := tmpFile.Close(); err != nil {
			_ = os.Remove(tmpFile.Name())
			return err
		}

		if err := os.Rename(tmpFile.Name(), finalName); err != nil {
			_ = os.Remove(tmpFile.Name())
			return err
		}
	}
	return nil
}

func (ls *LocalStorage) ReadIntermediateForReduce(reduceId int, nMap int) ([]KeyValue, error) {
	var kva []KeyValue

	for m := range nMap {
		filename := fmt.Sprintf("job/%s/intermediate/mr-%d-%d", ls.jobId, m, reduceId)
		file, err := os.Open(filename)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return []KeyValue{}, err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				file.Close()
				return []KeyValue{}, err
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva, nil
}

func (ls *LocalStorage) WriteOutput(reduceId int, kvs []KeyValue) error {
	finalName := fmt.Sprintf("job/%s/output/mr-out-%d", ls.jobId, reduceId)
	dir := filepath.Dir(finalName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmpFile, err := os.CreateTemp(".", "mr-out-temp-*")
	if err != nil {
		return err
	}

	for _, kv := range kvs {
		_, err := fmt.Fprintf(tmpFile, "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			return err
		}
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return err
	}

	if err := os.Rename(tmpFile.Name(), finalName); err != nil {
		os.Remove(tmpFile.Name())
		return err
	}

	return nil
}
