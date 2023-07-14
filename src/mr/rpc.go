package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	Task Task
}

type ReportTaskDoneArgs struct {
	TaskId   int
	TaskType TaskType
}
type ReportTaskDoneReply struct {
	Succeeded bool
}

// Add your RPC definitions here.
type Task struct {
	FileName string
	Id       int
	// map task have 1 file, reduce task have nMap files
	Files     []string
	StartTime time.Time
	Type      TaskType
	Status    TaskStatus
	workerId  int

	NReduce int
}

type TaskType int

const (
	NoneTask TaskType = iota
	MapTask
	ReduceTask
	WaitTask
	Exit
)

type TaskStatus int

const (
	New TaskStatus = iota
	Running
	Done
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
