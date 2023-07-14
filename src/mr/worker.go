package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (k ByKey) Len() int {
	return len(k)
}

func (k ByKey) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

func (k ByKey) Less(i, j int) bool {
	return k[i].Key < k[j].Key
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// trying to get a task from the coordinator continuously
	for {
		// call the coordinator to get a task
		// if the task is a map task, then call the map function
		// if the task is a reduce task, then call the reduce function
		// if the task is an exit task, then exit
		newTask := GetTask()
		switch newTask.Type {
		case MapTask:
			// do map
			//log.Printf("do map task %d\n", newTask.Id)
			doMap(newTask, mapf)
		case ReduceTask:
			// do reduce
			// log.Printf("do reduce task %d\n", newTask.Id)
			doReduce(newTask, reducef)
		case WaitTask:
			// wait
			time.Sleep(time.Second)
		case Exit:
			// exit
			return
		}
	}
}

func doMap(newTask Task, mapf func(string, string) []KeyValue) {
	f := newTask.Files[0]
	file, _ := os.Open(f)
	content, _ := ioutil.ReadAll(file)

	// call the map function and write the result to the intermediate file
	intermediate := mapf(f, string(content))
	byReduceFiles := make(map[int][]KeyValue)
	for _, kv := range intermediate {
		reduceNum := ihash(kv.Key) % newTask.NReduce
		byReduceFiles[reduceNum] = append(byReduceFiles[reduceNum], kv)
	}

	//after map, write the intermediate kv pairs to the intermediate files
	files := make([]string, newTask.NReduce)
	for reduceId, kvs := range byReduceFiles {
		fileName := fmt.Sprintf("%d-mr-%d-%d", os.Getpid(), newTask.Id, reduceId)
		files[reduceId] = fileName
		file, err := os.Create(fileName)
		if err != nil {
			log.Printf("cannot create %v", fileName)
		}
		// write the intermediate kv pairs to the file
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("cannot encode %v", kv)
			}
		}
		file.Close()
	}

	// call the coordinator to report the task is done
	if ReportTaskDone(newTask.Id, MapTask) {
		pid := os.Getpid()
		prefix := fmt.Sprintf("%d-mr", pid)

		files, _ := ioutil.ReadDir(".")
		for _, file := range files {
			if strings.HasPrefix(file.Name(), prefix) {
				trimmedName := strings.TrimPrefix(file.Name(), strconv.Itoa(pid)+"-")
				os.Rename(file.Name(), trimmedName)
			}
		}
	}
}

func doReduce(newTask Task, reducef func(string, []string) string) {
	// recover the intermediate kv pairs
	var intermediate []KeyValue
	for _, filename := range newTask.Files {
		file, err := os.Open(filename)
		if err != nil {
			// it is common for some Reduces to have no intermediate files, so we ignore the file not exist error
			if !os.IsNotExist(err) {
				log.Printf("cannot open %v", filename)
			}
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("%d-mr-out-%d", os.Getpid(), newTask.Id)

	ofile, _ := os.Create(oname)
	for i := 0; i < len(intermediate); {
		// aggregate the values with the same key
		j := i
		var values []string
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			values = append(values, intermediate[j].Value)
			j++
		}

		// call reducef
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, reducef(intermediate[i].Key, values))
		i = j
	}
	ofile.Close()

	if ReportTaskDone(newTask.Id, ReduceTask) {
		// fmt.Printf("worker: finish reduce task %d\n", newTask.Id)
		pid := os.Getpid()
		prefix := fmt.Sprintf("%d-mr-out", pid)

		files, _ := ioutil.ReadDir(".")
		for _, file := range files {
			if strings.HasPrefix(file.Name(), prefix) {
				trimmedName := strings.TrimPrefix(file.Name(), strconv.Itoa(pid)+"-")
				os.Rename(file.Name(), trimmedName)
			}
		}
	}
}

func GetTask() Task {
	args := &GetTaskArgs{os.Getpid()}
	reply := &GetTaskReply{}
	if !call("Coordinator.GetTask", args, reply) {
		fmt.Printf("worker: GetTask call failed!\n")
		return Task{}
	} else if reply.Task.Type == 0 {
		fmt.Printf("worker: something wrong with coordinator!\n")
	}

	return reply.Task
}

func ReportTaskDone(taskId int, taskType TaskType) bool {
	args := ReportTaskDoneArgs{}
	args.TaskId = taskId
	args.TaskType = taskType
	reply := ReportTaskDoneReply{}
	if !call("Coordinator.TaskDone", &args, &reply) {
		fmt.Printf("worker: ReportTaskDone call failed!\n")
		os.Exit(0)
	}
	return reply.Succeeded
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
