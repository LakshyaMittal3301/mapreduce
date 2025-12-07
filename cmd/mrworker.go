package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"flag"
	"fmt"
	"log"
	"os"
	"plugin"

	mr "github.com/LakshyaMittal3301/mapreduce/mapreduce"
)

func main() {

	coordAddr := flag.String("coord-addr", "localhost:8123", "address of the coordinator")
	app := flag.String("app", "", "path to the app/plugin (.so file)")
	backend := flag.String("storage", "local", "storage backend: local|s3")
	s3BucketFlag := flag.String("s3-bucket", "", "S3 bucket name")

	flag.Parse()

	if *app == "" {
		fmt.Fprintf(os.Stderr, "Usage: mrworker -coord-addr=<addr> -app=<plugin.so>\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(*app)
	var storage mr.Storage

	switch *backend {
	case "local":
		storage = mr.NewLocalStorage()
	case "s3":
		s3Store, err := mr.NewS3Storage(*s3BucketFlag)
		if err != nil {
			log.Fatalf("failed to init S3 storage: %v", err)
		}
		storage = s3Store
	default:
		log.Fatalf("unknown storage backend %s", *backend)
	}

	mr.Worker(mapf, reducef, *coordAddr, storage)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
