package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

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
	mu    sync.Mutex
	JobId string

	ListenAddr string
	Files      []string
	listener   net.Listener
	stopOnce   sync.Once
	stopped    atomic.Bool

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
	l, e := net.Listen("tcp", c.ListenAddr)
	c.listener = l
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Stop closes the RPC listener so the port can be reused once the job ends.
func (c *Coordinator) Stop() {
	c.stopOnce.Do(func() {
		if c.listener != nil {
			_ = c.listener.Close()
		}
		c.stopped.Store(true)
	})
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
func MakeCoordinator(files []string, nReduce int, jobId string, listenAddr string) *Coordinator {
	c := Coordinator{
		JobId:        jobId,
		ListenAddr:   listenAddr,
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
