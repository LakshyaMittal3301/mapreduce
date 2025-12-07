package mr

import "time"

type Tuning struct {
	MapTaskTimeout    time.Duration
	ReduceTaskTimeout time.Duration
	WorkerIdleWait    time.Duration
	S3MaxConcurrency  int
}

var tuning = Tuning{
	MapTaskTimeout:    10 * time.Second, // default for local
	ReduceTaskTimeout: 10 * time.Second, // default for local+S3
	WorkerIdleWait:    100 * time.Millisecond,
	S3MaxConcurrency:  16,
}

func SetTuning(t Tuning) {
	tuning = t
}

func TuningConfig() Tuning {
	return tuning
}
