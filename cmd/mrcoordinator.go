package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"flag"
	"fmt"
	"log"

	"os"
	"time"

	mr "github.com/LakshyaMittal3301/mapreduce/mapreduce"
)

func main() {
	// if len(os.Args) < 2 {
	// 	fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
	// 	os.Exit(1)
	// }

	nReduce := flag.Int("n-reduce", 10, "number of workers to use")
	jobId := flag.String("job-id", "", "job identifier prefix")
	listenAddr := flag.String("listen", ":8123", "address to listen for worker RPCs")
	logLevel := flag.String("log-level", "info", "log level: info|debug")

	flag.Parse()

	unique := time.Now().UnixNano()

	finalJobId := fmt.Sprintf("%s-%d", *jobId, unique)

	if *jobId == "" {
		finalJobId = fmt.Sprintf("%s-%d", "job", unique)
	}

	inputFiles := flag.Args()

	if len(inputFiles) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator [flags] inputfiles...\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	mr.SetLogLevel(*logLevel)
	log.Printf("Starting coordinator for job id: %s", finalJobId)

	start := time.Now()

	m := mr.MakeCoordinator(inputFiles, *nReduce, finalJobId, *listenAddr)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	mr.Infof("Coordinator: job %s completed in %s", finalJobId, time.Since(start))
	m.Stop()
	time.Sleep(time.Second)
}
