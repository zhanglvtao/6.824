package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
type JobStage string
const (
  JobStageMap 		JobStage = "Map"
  JobStageReduce 	JobStage = "Reduce" 
  JobStageFinish  JobStage = "Finish"
)
type Coordinator struct {
  // Your definitions here.
  stage								JobStage
  eMutex							*sync.Mutex
  executors						map[ExecutorId]*Executor
  taskManager   			*TaskManager
  executorIdCount     *uint64
  nReduce             uint64
}

func (c *Coordinator) executorCheck() {
  for {
    c.eMutex.Lock()
    log.Println("====> Executor Check")
    for executorId := range c.executors {
      executor := c.executors[executorId]
      if executor.status == ExecutorStatusDead {
        delete(c.executors, executorId)
        log.Printf("Remove Dead Executor-%v", executor.id)
      }
      now := UnixTimeStamp(time.Now().Unix())
      if (executor.lastHeartbeatTime + 15 < now ||
        (executor.status == ExecutorStatusDead && executor.taskId != INVALID_TASK_ID)) {
        executor.status = ExecutorStatusDead
        log.Printf("Executor-%d Dead ☠️ . Due Now/LUT (%v/%v)", executorId, now, executor.lastHeartbeatTime)
        go c.taskManager.ReSchedule(executor.taskId)
      }
    }   
    c.eMutex.Unlock()
    log.Println("<==== Finish Executor Check")
    time.Sleep(5 * time.Second)
  }
}

func (c *Coordinator) stageCheck() {
  for {
    time.Sleep(5 * time.Second)
    log.Println("====> Start Stage Check")
    if c.taskManager.IsReduceDone() && c.stage == JobStageReduce {
      log.Println("Stage: Reduce => Finish")
      c.stage = JobStageFinish
    } else if c.taskManager.IsMapDone() && c.taskManager.IsShuffleDone() && c.stage == JobStageMap {
      log.Println("Stage: Map => Reduce")
      // Generate Reduce Task
      for i := 0; i < int(c.nReduce); i++ {
        shuffle := "shuffle-" + strconv.Itoa(i)
        reduce := "mr-out-" + strconv.Itoa(i)
        task := Task{
          Id: INVALID_TASK_ID,
          Input: shuffle,
          Output: reduce,
          Type: TaskTypeReduce,
          Status: TaskStatusReady,
        }
        c.taskManager.Add(&task)
        log.Printf("Add %v Task-%v", task.Type, task)
      }
      c.stage = JobStageReduce
    }
    log.Println("<==== Finish Stage Check")
  }
}
// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
  reply.Y = args.X + 1
  return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
  executor := &Executor{
    id: ExecutorId(atomic.AddUint64(c.executorIdCount, 1)),
    lastHeartbeatTime: UnixTimeStamp(time.Now().Unix()),
    status: ExecutorStatusAlive,
  }
  c.eMutex.Lock()
  c.executors[executor.id] = executor
  c.eMutex.Unlock()
  reply.ExecutorId = executor.id
  log.Printf("Executor-%d Register At %d", executor.id, executor.lastHeartbeatTime)
  return nil
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
  executor, ok := c.executors[args.ExecutorId]
  if !ok {
    log.Printf("Executor-%d Not Exists", args.ExecutorId)
    return nil
  }
  executor.lastHeartbeatTime = UnixTimeStamp(time.Now().Unix())
  if c.JobDone() {
    reply.Action = HeartbeatActionExit
    delete(c.executors, args.ExecutorId)
  } else {
    reply.Action = HeartbeatActionNoAction
  }
  log.Printf("Executor-%d Heartbeat ❤️ At %d", executor.id, executor.lastHeartbeatTime)
  return nil
}

func (c *Coordinator) FetchTask(args* FetchTaskArgs, reply *FetchTaskReply) error {
  taskId := c.taskManager.Schedule()
  if taskId == INVALID_TASK_ID {
    log.Println("Have No Ready Task To Schedule!")
    return nil
  }
  task := c.taskManager.Get(taskId)
  executor := c.executors[args.ExecutorId]
  executor.taskId = task.Id
  reply.Task = task
  log.Printf("Executor-%v Fetch Task-%v", executor.id, task)
  return nil
}

func (c *Coordinator) UpdateTask(args* UpdateTaskArgs, reply *UpdateTaskReply) error {
  if !c.taskManager.Update(args.Task) {
    log.Printf("Executor-%d Update Task-%v Fail", args.ExecutorId, args.Task)
    return nil
  }
  reply.Task = c.taskManager.Get(args.Task.Id)
  log.Printf("Executor-%d Update Task-%v Success", args.ExecutorId, args.Task)
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
func (c *Coordinator) JobDone() bool {
  return c.stage == JobStageFinish 
}

func (c *Coordinator) Done() bool {
  r := c.JobDone() && len(c.executors) == 0
  if r {
    log.Println("Coordinator Exit Bye Bye 👋")
  }
  return r
}


func (c *Coordinator) DispatchBackground() {
  go c.stageCheck()
  go c.executorCheck()
  go c.taskManager.Monitor()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
  log.SetFlags(log.Ldate | log.Lshortfile)
  log.SetPrefix("Coordinator ")
  os.Remove("coordinator.log")
  c := Coordinator{
    stage: JobStageMap,
    eMutex: &sync.Mutex{},
    executors: make(map[ExecutorId]*Executor),
    taskManager: makeTaskManager(),
    executorIdCount: new(uint64),
    nReduce: uint64(nReduce),}
  // gen map task
  for _, file := range files {
    task := Task { 
      Id: INVALID_TASK_ID, 
      Input: file, 
      Output: file + "-m",
      Type: TaskTypeMap,
      Status: TaskStatusReady,
      NReduce: c.nReduce}
    if !c.taskManager.Add(&task) {
      log.Fatal("Add Task Fail")
    }
    log.Println("Add Task:", task)
  }
  // go background check
  c.DispatchBackground()
  c.server()
  return &c
}
