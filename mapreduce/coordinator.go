package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Init Phase
// len(files) is the number of map tasks
// nReduce is numnber of reduce tasks
// Job has phases: Map, Reduce, Done
// Workers will constantly ask for tasks -> {Map, Reduce, Idle, Exit}

// Task -> Map
// 1. Assign worker a map task, along with a file, and the id (which could just be the file number (0 -> n-1))
// 2,

// Task -> Map
// 1. Worker will ask for a task
// 2. Assign a map task -> By sending file locations, task type, etc.
// 3. Wait for the worker to inform whether the task is done or not, along with the location of intermediate file.
// 4. If 10 seconds pass, assume worker is dead, and assign the task to someone else.
// 5. Once all Map tasks are done

// Task -> Reduce
// 1. Worker will ask for a task
// 2. Assign a map task -> By sending file locations, task type, etc.
// 3. Wait for the worker to inform whether the task is done or not.
// 4. If 10 seconds pass, assume worker is dead, and assign the task to someone else.
// 5. Once all reduce tasks are done, mark the complete job done. And can return true from Done().

// Keep a map of map task to status -> {idle, in-progress, completed}
// When a GetTask request comes, iterate over the map and assign the first idle task
// Mark it in-progress
// Start a timer for 10s, that will check if the task is completed or not.
// If still in-progress, mark it as idle.

// When a ReportTaskDone is received, mark the task to be completed (it could either be idle or in-progress or completed, due to timeouts or multiple assignments)
// When the status is changed from idle / in-progress to completed, increase the count of map tasks done by 1
// If the count reaches NMap then, change the phase to Reduce tasks

type Phase string

const (
	PhaseMap    Phase = "Map"
	PhaseReduce Phase = "Reduce"
	PhaseDone   Phase = "Done"
)

type TaskStatus string

const (
	TaskStatusIdle       TaskStatus = "Idle"
	TaskStatusInProgress TaskStatus = "InProgress"
	TaskStatusCompleted  TaskStatus = "Completed"
)

type Task struct {
	Status    TaskStatus
	StartTime time.Time
}

type Coordinator struct {
	mu           sync.Mutex
	Files        []string
	NMap         int
	NReduce      int
	CurrentPhase Phase

	MapTasks    []Task
	ReduceTasks []Task

	MapTasksDone    int
	ReduceTasksDone int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	switch c.CurrentPhase {
	case PhaseMap:
		err = c.assignMapTask(reply)
	case PhaseReduce:
		err = c.assignReduceTask(reply)
	case PhaseDone:
		err = c.assignPhaseDone(reply)
	default:
		err = fmt.Errorf("corrupted phase of coordinator: %v", c.CurrentPhase)
	}
	if err != nil {
		// log.Printf("coordinator: %v\n", err)
	}
	return err
}

func (c *Coordinator) assignMapTask(reply *GetTaskReply) error {
	for idx := range c.MapTasks {
		if isTaskIdle(&c.MapTasks[idx]) {
			reply.Type = TaskTypeMap
			reply.Map = &MapTaskInfo{
				ID:       idx,
				Filename: c.Files[idx],
				NReduce:  c.NReduce,
			}
			c.MapTasks[idx].Status = TaskStatusInProgress
			c.MapTasks[idx].StartTime = time.Now()
			return nil
		}
	}
	reply.Type = TaskTypeIdle
	return nil
}

func (c *Coordinator) assignReduceTask(reply *GetTaskReply) error {
	for idx := range c.ReduceTasks {
		if isTaskIdle(&c.ReduceTasks[idx]) {
			reply.Type = TaskTypeReduce
			reply.Reduce = &ReduceTaskInfo{
				ID:    idx,
				NMaps: c.NMap,
			}
			c.ReduceTasks[idx].Status = TaskStatusInProgress
			c.ReduceTasks[idx].StartTime = time.Now()
			return nil
		}
	}
	reply.Type = TaskTypeIdle
	return nil
}

func (c *Coordinator) assignPhaseDone(reply *GetTaskReply) error {
	if c.MapTasksDone == c.NMap && c.ReduceTasksDone == c.NReduce {
		reply.Type = TaskTypeExit
		return nil
	}
	return fmt.Errorf("incomplete tasks in done phase: (expected map tasks: %d, done map tasks: %d), (expected reduce tasks: %d, done reduce tasks: %d)", c.NMap, c.MapTasksDone, c.NReduce, c.ReduceTasksDone)
}

func isTaskIdle(task *Task) bool {
	if task.Status == TaskStatusInProgress && time.Since(task.StartTime) > 10*time.Second {
		task.Status = TaskStatusIdle
	}
	return task.Status == TaskStatusIdle
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error

	switch args.Type {
	case TaskTypeMap:
		err = c.markMapTaskDone(args)
	case TaskTypeReduce:
		err = c.markReduceTaskDone(args)
	default:
		err = fmt.Errorf("unexpected task type for report task done: %v", args.Type)
	}

	if err != nil {
		// log.Printf("coordinator: %v\n", err)
	}
	return err
}

func (c *Coordinator) markMapTaskDone(args *ReportTaskDoneArgs) error {
	idx := args.ID
	if c.MapTasks[idx].Status != TaskStatusCompleted {
		c.MapTasksDone += 1
		if c.MapTasksDone == c.NMap {
			c.CurrentPhase = PhaseReduce
		}
	}
	c.MapTasks[idx].Status = TaskStatusCompleted
	return nil
}

func (c *Coordinator) markReduceTaskDone(args *ReportTaskDoneArgs) error {
	idx := args.ID
	if c.ReduceTasks[idx].Status != TaskStatusCompleted {
		c.ReduceTasksDone += 1
		if c.ReduceTasksDone == c.NReduce {
			c.CurrentPhase = PhaseDone
		}
	}
	c.ReduceTasks[idx].Status = TaskStatusCompleted
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.CurrentPhase == PhaseDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:        files,
		NMap:         len(files),
		NReduce:      nReduce,
		CurrentPhase: PhaseMap,

		MapTasks:    make([]Task, len(files)),
		ReduceTasks: make([]Task, nReduce),
	}

	for i := range c.MapTasks {
		c.MapTasks[i].Status = TaskStatusIdle
	}

	for i := range c.ReduceTasks {
		c.ReduceTasks[i].Status = TaskStatusIdle
	}
	c.server()
	return &c
}
