package mr

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce int64
	nMap    int64

	nReduceRunning int64
	nMapRunning    int64

	nReduceDone int64
	nMapDone    int64

	files       []string
	mapTasks    []Task
	reduceTasks []Task

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	var task *Task = nil

	if atomic.LoadInt64(&c.nMapDone) < c.nMap {
		task = c.selectTask(c.mapTasks, args.WorkerId)
		// all tasks are assigned, but not all tasks are done
		if task == nil {
			task = &Task{Type: WaitTask}
		}
		reply.Task = *task
		return nil
	}

	if atomic.LoadInt64(&c.nReduceDone) < c.nReduce {
		task = c.selectTask(c.reduceTasks, args.WorkerId)
		if task == nil {
			task = &Task{Type: WaitTask}
		}
		reply.Task = *task
		return nil
	}

	task = &Task{Type: Exit}
	reply.Task = *task
	return nil
}

func (c *Coordinator) selectTask(tasks []Task, workerId int) *Task {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range tasks {
		task := &tasks[i]
		if task.Status == Done {
			continue
		}
		if task.Status == Running {
			// reassign the task might cause the file with the same name being written twice
			// because some worker is fake dead, the correct thing to do is to use a temp file
			if time.Since(task.StartTime) > 15*time.Second {
				// log.Printf("type %d taskId %d timeout, reassign it\n", task.Type, task.Id)
				atomic.AddInt64(&c.nMapRunning, -1)
				task.Status = New
			}
		}
		if task.Status == New {
			if task.Type == MapTask {
				atomic.AddInt64(&c.nMapRunning, 1)
			} else if task.Type == ReduceTask {
				atomic.AddInt64(&c.nReduceRunning, 1)
			}
			task.Status = Running
			task.StartTime = time.Now()
			task.workerId = workerId
			return task
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	// log.Printf("Report task %d done\n", args.TaskId)
	var task *Task
	if args.TaskType == MapTask {
		task = &c.mapTasks[args.TaskId]
	} else if args.TaskType == ReduceTask {
		task = &c.reduceTasks[args.TaskId]
	} else {
		log.Printf("unknown task type %v", args.TaskType)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if task.Status != Running {
		// log.Printf("tasktype %d Id %d status %d: cannot set it done again", task.Type, task.Id, task.Status)
		reply.Succeeded = false
	} else {
		task.Status = Done
		reply.Succeeded = true
		if args.TaskType == MapTask {
			if args.TaskType == MapTask && c.nMap == atomic.LoadInt64(&c.nMapDone) {
				// all map tasks are done, wait 1 seconds for renaming temp files
				time.Sleep(1 * time.Second)
			}
			atomic.AddInt64(&c.nMapDone, 1)
			atomic.AddInt64(&c.nMapRunning, -1)
		} else if args.TaskType == ReduceTask {
			atomic.AddInt64(&c.nReduceDone, 1)
			atomic.AddInt64(&c.nReduceRunning, -1)
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.nMap == atomic.LoadInt64(&c.nMapDone) && c.nReduce == atomic.LoadInt64(&c.nReduceDone)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:     int64(nReduce),
		nMap:        int64(len(files)),
		files:       files,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	// map task
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = Task{
			Id:     i,
			Status: New,
			Type:   MapTask,
			Files:  []string{files[i]},

			NReduce: nReduce,
		}
	}

	// reduce task
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Id:     i,
			Status: New,
			Type:   ReduceTask,
			Files:  []string{},
		}

		for j := 0; j < len(files); j++ {
			c.reduceTasks[i].Files = append(c.reduceTasks[i].Files, fmt.Sprintf("mr-%d-%d", j, i))
		}
	}

	c.server()
	return &c
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
