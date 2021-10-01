package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)
func heartbeatLoop() {
	// call RPC heartbeat()
	for {
		Heartbeat()
		log.Printf("Call Heartbeat ❤️")
		time.Sleep(5 * time.Second)
	}
}

func workLoop() {
	for {
		if !live {
			log.Println("Worker Exit Bye Bye 👋")
			return 
		}
		localProcessTask = FetchTask()
		log.Println("Called FetchTask")
		if localProcessTask.Id == INVALID_TASK_ID {
			log.Println("Fetched Invalid Task, Retry")
			time.Sleep(3 * time.Second)
			continue
		}
		log.Println("Fetched Task:", localProcessTask)
		switch localProcessTask.Type {
		case TaskTypeMap: 
			processMap(&localProcessTask)
		case TaskTypeShuffle:
			processShuffle(&localProcessTask)
		default :
			processReduce(&localProcessTask)
		}
		localProcessTask.Status = TaskStatusDone
		UpdateTask()
		log.Printf("Updated Task-%v", localProcessTask)		
		time.Sleep(1 * time.Second)
	}
}

func processMap(task *Task) {
	log.Printf("Start To Process Task-%v", localProcessTask)
	input, err := os.Open(task.Input)
	if err != nil {
		log.Printf("cannot open %v err: %v", task.Input, err)
	}
	content, err := ioutil.ReadAll(input)
	if err != nil {
		log.Fatalf("cannot read %v err: %v", task.Input, err)
	}
	defer input.Close()
	kva := localMapf(task.Input, string(content))
	out, err := os.Create(task.Output)
	if err != nil {
		log.Fatalf("Map Task Out Put File %s Create Error %v", task.Output, err)
	}
	defer out.Close()
	for _, kv := range kva {
		out.WriteString(kv.Key + " " + kv.Value + "\n")
	}
	log.Printf("Miderate File Written To %s", task.Output)
}

func processShuffle(task *Task) {
	input, err := os.Open(task.Input)
	if err != nil {
		log.Fatalf("cannot open %v, Bye Bye 👋", task.Input)
	}
	defer input.Close()
	scanner := bufio.NewScanner(input)
	
	for scanner.Scan() {
		line := scanner.Text()
		kv := strings.Fields(line)
		key := kv[0]
		fileIdx := ihash(key) % int(task.NReduce)
		output := "shuffle-" + strconv.Itoa(fileIdx)
		out, err := os.OpenFile(output, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0777)
		if err != nil {
			log.Fatalf("Fail To Open %s To Write Shuffle Result, Bye Bye 👋", output)
		}
		out.WriteString(line + "\n")
		out.Close()
	}
	os.Remove(task.Input)
}

func processReduce(task *Task) {
	input, err := os.Open(task.Input)
	if err != nil {
		log.Printf("File %v Not Exists", task.Input)
		return
	}
	defer input.Close()
	output, err := os.OpenFile(task.Output, os.O_CREATE | os.O_WRONLY , 0777)
	if err != nil {
		log.Fatalf("Fail To Open %s To Write Reduce Result", task.Output)
	}
	defer output.Close()
	scanner := bufio.NewScanner(input)
	keyMap := make(map[string][]string)
	for scanner.Scan() {
		line := scanner.Text()
		kv := strings.Fields(line)
		key := kv[0]
		value := kv[1]
		keyMap[key] = append(keyMap[key], value)
	}
	for k,v := range keyMap {
		result := localReducef(k, v)
		output.WriteString(k + " " + result + "\n")
	}
	os.Remove(task.Input)
}

// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
var localExecutorId ExecutorId
var localProcessTask Task
var live = true
var localMapf func(string, string) []KeyValue
var localReducef func(string, []string) string

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetFlags(log.Ldate | log.Lshortfile)
	localMapf = mapf
	localReducef = reducef
	for {
		localExecutorId = Register()
		log.Printf("Called Register, Get ExecutorId %d", localExecutorId)
		if localExecutorId == INVALID_EXECUTOR_ID {
			log.Printf("Call Register Fail, Sleep And Retry ...")
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}
	log.SetPrefix("Executor-" + strconv.Itoa(int(localExecutorId)) + " ")
	log.Println("Start Heartbeat Loop")
	go heartbeatLoop()
	log.Println("Start Work Loop")
	workLoop()
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func Register() ExecutorId {
	args := RegisterArgs{}
	reply := RegisterReply{}
	call("Coordinator.Register", &args, &reply)
	return reply.ExecutorId
}

func Heartbeat(){
	args := HeartbeatArgs{ ExecutorId: localExecutorId }
	reply := HeartbeatReply{}
	call("Coordinator.Heartbeat", &args, &reply)
	if reply.Action == HeartbeatActionExit {
			live = false
	}
}

func FetchTask() Task {
	args := FetchTaskArgs{ ExecutorId: localExecutorId }
	reply := FetchTaskReply{}
	call("Coordinator.FetchTask", &args, &reply)
	return reply.Task
}

func UpdateTask() Task {
	args := UpdateTaskArgs{ ExecutorId: localExecutorId, Task: localProcessTask}
	reply := UpdateTaskReply{}
	call("Coordinator.UpdateTask", &args, &reply)
	return reply.Task
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
