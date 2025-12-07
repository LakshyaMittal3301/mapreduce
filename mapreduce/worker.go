package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"sort"
	"time"
)

var coordinatorAddress string
var storage Storage

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, coordAddr string, _storage Storage) {
	coordinatorAddress = coordAddr
	storage = _storage

	for {

		reply, ok := pollGetTask()

		if !ok {
			// log.Printf("worker: could not reach coordinator, exiting\n")
			return
		}
		if reply.Type == TaskTypeExit {
			// log.Printf("worker: got exit task, exiting\n")
			return
		}

		err := handleTask(reply, mapf, reducef)
		if err != nil {
			log.Printf("worker: error occured while handling task: %v\n, sleeping!", err)
			time.Sleep(time.Second * 2)
			return
		}
	}
}

func pollGetTask() (GetTaskReply, bool) {
	args := GetTaskArgs{}
	const idleWait = time.Millisecond * 100

	for {
		reply, ok := callGetTask(args)
		if !ok {
			return GetTaskReply{}, ok
		}
		if reply.Type == TaskTypeIdle {
			// log.Printf("worker: Idle recieved, sleeping for: %ds\n", idleWait/time.Second)
			time.Sleep(idleWait)
		} else {
			return reply, ok
		}
	}
}

func handleTask(reply GetTaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) error {
	var err error
	args := ReportTaskDoneArgs{}
	// log.Printf("Worker: setting jobId: %s\n", reply.JobId)
	storage.SetJob(reply.JobId)

	switch reply.Type {
	case TaskTypeMap:
		err = handleMapTask(reply.Map, mapf)
		args.Type = TaskTypeMap
		args.ID = reply.Map.ID

	case TaskTypeReduce:
		err = handleReduceTask(reply.Reduce, reducef)
		args.Type = TaskTypeReduce
		args.ID = reply.Reduce.ID

	default:
		err = fmt.Errorf("unexpected task type recieved: %v", reply.Type)
	}

	if err != nil {
		return err
	}

	_, ok := callReportTaskDone(args)
	if !ok {
		return fmt.Errorf("error in calling report task done with args: %v", args)
	}
	return nil
}

func handleMapTask(taskInfo *MapTaskInfo, mapf func(string, string) []KeyValue) error {
	if taskInfo == nil {
		return fmt.Errorf("no map task information found")
	}

	content, err := storage.ReadInput(taskInfo.Filename)
	if err != nil {
		return err
	}

	kva := mapf(taskInfo.Filename, content)

	buckets := make([][]KeyValue, taskInfo.NReduce)

	for _, kv := range kva {
		hash := ihash(kv.Key) % taskInfo.NReduce
		buckets[hash] = append(buckets[hash], kv)
	}

	err = storage.WriteIntermediate(taskInfo.ID, taskInfo.NReduce, buckets)
	if err != nil {
		return err
	}
	return nil
}

func handleReduceTask(taskInfo *ReduceTaskInfo, reducef func(string, []string) string) error {
	if taskInfo == nil {
		return fmt.Errorf("worker: no reduce task information found")
	}
	kva, err := storage.ReadIntermediateForReduce(taskInfo.ID, taskInfo.NMaps)

	if err != nil {
		return err
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	finalKV := []KeyValue{}
	idx := 0
	for idx < len(kva) {
		key := kva[idx].Key
		values := []string{}
		for idx < len(kva) && kva[idx].Key == key {
			values = append(values, kva[idx].Value)
			idx++
		}
		finalValue := reducef(key, values)
		finalKV = append(finalKV, KeyValue{
			Key:   key,
			Value: finalValue,
		})
	}

	err = storage.WriteOutput(taskInfo.ID, finalKV)
	if err != nil {
		return err
	}

	return nil
}

func callGetTask(args GetTaskArgs) (GetTaskReply, bool) {
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	return reply, ok
}

func callReportTaskDone(args ReportTaskDoneArgs) (ReportTaskDoneReply, bool) {
	reply := ReportTaskDoneReply{}
	ok := call("Coordinator.ReportTaskDone", &args, &reply)
	return reply, ok
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", coordinatorAddress)
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
