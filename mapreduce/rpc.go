package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Add your RPC definitions here.
type TaskType string

const (
	TaskTypeMap    TaskType = "Map"
	TaskTypeReduce TaskType = "Reduce"
	TaskTypeIdle   TaskType = "Idle"
	TaskTypeExit   TaskType = "Exit"
)

type GetTaskArgs struct{}

type MapTaskInfo struct {
	ID       int
	Filename string
	NReduce  int
}

type ReduceTaskInfo struct {
	ID    int
	NMaps int
}

type GetTaskReply struct {
	JobId  string
	Type   TaskType
	Map    *MapTaskInfo
	Reduce *ReduceTaskInfo
}

type ReportTaskDoneArgs struct {
	ID   int
	Type TaskType
}

type ReportTaskDoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
// func coordinatorSock() string {
// 	s := "/var/tmp/5840-mr-"
// 	s += strconv.Itoa(os.Getuid())
// 	return s
// }
