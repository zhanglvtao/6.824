package mr

import (
	"container/list"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)
type TaskStatus string
type TaskId   uint64
type TaskType string 
const INVALID_TASK_ID TaskId = 0
const (
  TaskTypeMap        TaskType = "TypeMap"
  TaskTypeShuffle    TaskType = "TypeShuffle"
  TaskTypeReduce     TaskType = "TypeReduce"
)
const (
  TaskStatusReady     TaskStatus = "StatusReady"
  TaskStatusWorking   TaskStatus = "StatusWorking"
  TaskStatusDone      TaskStatus = "StatusDone"
)
type Task struct {
  Id       TaskId
  Input    string
  Output   string
  Status   TaskStatus
  Type     TaskType
  NReduce  uint64
  Version  uint64
}

type TaskManager struct {
  mapGroup      *TaskIdGroup
  shuffleGroup  *TaskIdGroup
  reduceGroup   *TaskIdGroup
  taskIdx       map[TaskId]*Task
  taskIdCount   *uint64
}

func makeTaskManager() *TaskManager{
  return &TaskManager{
    mapGroup: makeTaskIdGroup(),
    shuffleGroup: makeTaskIdGroup(),
    reduceGroup: makeTaskIdGroup(),
    taskIdx: make(map[TaskId]*Task),
    taskIdCount: new(uint64),}
}

func (taskManager *TaskManager) Monitor() {
  go taskManager.MonitorGroup(taskManager.mapGroup, TaskTypeMap)
  go taskManager.MonitorGroup(taskManager.shuffleGroup, TaskTypeShuffle)
  go taskManager.MonitorGroup(taskManager.reduceGroup, TaskTypeReduce)
  go taskManager.MonitorTask()
}

func (taskManager *TaskManager) MonitorTask() {
  for {
    log.Println("Start Monitor Task")
    // remove done MAP task then gen ready SHUFFLE task
    doneMapTaskIds := taskManager.mapGroup.RemoveDoneTaskIds()
    versionMap := make(map[uint64]*list.List)
    taskList := list.New()
    for _, doneId := range doneMapTaskIds {
      doneTask, ok := taskManager.taskIdx[doneId]
      if !ok {
        log.Fatalf("Task Manger Have No Such Task-%v", doneId)
      }

      li, ok := versionMap[doneTask.Version]
      if !ok {
        li = list.New()
        versionMap[doneTask.Version] = li
      }
      li.PushBack(doneTask)
      // Gen Shuffle Task From Map Task
      task := Task{
        Id: TaskId(atomic.AddUint64(taskManager.taskIdCount, 1)),
        Input: doneTask.Output,
        Status: TaskStatusReady,
        Type: TaskTypeShuffle,
        NReduce: doneTask.NReduce,
        Version: doneTask.Version}
      // Only This Place Do Put To taskIdx ?
      taskManager.taskIdx[task.Id] = &task
      taskList.PushBack(task.Id)
      log.Println("Add Shuffle Task:", task)
    }
    taskManager.shuffleGroup.PushBackListToReady(taskList)
    for _, value := range versionMap {
      taskManager.mapGroup.AddToHistory(value)
    }
    log.Println("Finish Monitor Task, Sleep 5 Seconds")
    time.Sleep(5 * time.Second)
  }
}
func (taskManager *TaskManager) MonitorGroup(taskGroup *TaskIdGroup, taskType TaskType) {
  for {  
    log.Printf("Start To Monitor Group-%v Ready(%v) Working(%v) Done(%v) History(%v)", taskType, taskGroup.ReadySize(), taskGroup.WorkingSize(), taskGroup.DoneSize(), taskGroup.HistorySize())
    taskGroup.wMutex.Lock()
    toDone := make([]TaskId, 4)
    // traverse working
    for taskId := range taskGroup.working {
      if taskId == INVALID_TASK_ID {
        log.Fatalf("TaskId-%d With No According Task", taskId)
      }
      task := taskManager.Get(taskId)
      if task.Status == TaskStatusDone {
        toDone = append(toDone, taskId)
      }
    }
    taskGroup.wMutex.Unlock()
    for _, id := range toDone {
      if taskGroup.WorkingToDone(id) {
        log.Printf("Task-%d Working => Done", id)
      }
    }
    log.Println("Finish Monitor Group, Sleep 5 Seconds")
    time.Sleep(5 * time.Second)
  }  
}

func (taskManager *TaskManager) Add(task *Task) bool {
  task.Id = TaskId(atomic.AddUint64(taskManager.taskIdCount, 1))
  switch task.Type {
    case TaskTypeMap:
      taskManager.mapGroup.PushBackToReady(task.Id)
    case TaskTypeReduce:
      taskManager.reduceGroup.PushBackToReady(task.Id)
    case TaskTypeShuffle:
      taskManager.shuffleGroup.PushBackToReady(task.Id)
  }
  taskManager.taskIdx[task.Id] = task
  return true
}
func (taskManager *TaskManager) Get(taskID TaskId) Task {
  return *taskManager.taskIdx[taskID]
}

func (taskManger *TaskManager) Update(task Task) bool {
  oldTask , ok:= taskManger.taskIdx[task.Id]
  log.Printf("Update Task From %v To %v", oldTask, task)
  if !ok {
    log.Printf("Task %d Not Exists", task.Id)
    return false
  }
  oldTask.Status = task.Status
  return true
}

func (taskManager *TaskManager) Schedule() TaskId {
  var taskIdGroup *TaskIdGroup
  if taskManager.mapGroup.ReadySize() != 0 {
    taskIdGroup = taskManager.mapGroup
  }  else if taskManager.shuffleGroup.ReadySize() != 0 {
    taskIdGroup = taskManager.shuffleGroup
  } else {
    taskIdGroup = taskManager.reduceGroup
  }
  taskId := INVALID_TASK_ID
  if !taskIdGroup.RetriveReadyTaskID(&taskId) {
    return taskId
  }
  log.Printf("Remove TaskId-%v From Ready List", taskId)
  res := taskIdGroup.ToWorking(taskId)
  log.Printf("Add TaskId-%v To Working Set %v", taskId, res)
  taskManager.taskIdx[taskId].Status = TaskStatusWorking
  return taskId
}

func (taskManager *TaskManager) ReSchedule(taskId TaskId) {
  if taskId == INVALID_TASK_ID {
    log.Printf("Invalid Task Id, No Need To Reschedule")
  }
  task, ok := taskManager.taskIdx[taskId]
  if !ok {
    log.Printf("Task-%v Not Exsits, ReSchedule No Task!", taskId)
    return
  }
  var taskIndex *TaskIdGroup
  switch task.Type {
    case TaskTypeMap :
      taskIndex = taskManager.mapGroup
    case TaskTypeReduce :
      taskIndex = taskManager.reduceGroup
    default :
      // Speical Handle For Shuffle Task Reschdule 
      // We Redo All Shuffle Task !
      taskManager.RedoShuffle(task.Version)
      return
  }
  switch task.Status {
    case TaskStatusReady :
      log.Printf("Reschedule Task-%d Already In Ready, Do Nothing", task.Id) 
    case TaskStatusDone :
      log.Printf("Reschedule Task-%d Already Done, Do Nothing", task.Id)
    default : 
      log.Printf("Reschedule Task-%d From Working To Ready", task.Id)
       task := taskManager.taskIdx[task.Id]
      task.Status = TaskStatusReady
			taskIndex.WorkingToReady(task.Id)
  }
}

func (taskManager *TaskManager) IsDone() bool {
  return taskManager.IsMapDone() && 
  taskManager.IsShuffleDone() && 
  taskManager.IsReduceDone()
}

func (taskManager *TaskManager) IsMapDone() bool {
  return taskManager.mapGroup.IsAllDone()
}

func (taskManger *TaskManager) IsShuffleDone() bool {
  return taskManger.shuffleGroup.IsAllDone()
}

func (taskManager *TaskManager) IsReduceDone() bool {
  return taskManager.reduceGroup.IsAllDone()
}

func (taskManager *TaskManager) RedoShuffle(failVersion uint64) {
  toBeReShuffleIdList := taskManager.mapGroup.RecoverFromHistory()
  if toBeReShuffleIdList == nil {
    log.Printf("❗️Not Exists Version-%v Map Tasks", failVersion)
    return
  }

  taskManager.shuffleGroup.ClearWorking()
  taskManager.shuffleGroup.ClearReady()
  taskManager.shuffleGroup.ClearDone()
	for i := 0; i < 10; i++ {
		os.Remove(fmt.Sprintf("shuffle-%v", i))
	}
  log.Println("☎️ Clear Shuffle Group <Working> <Ready> <Done> And Remove Old Shuffle Files")

  taskManager.mapGroup.PushBackListToDone(toBeReShuffleIdList)
}

type TaskIdGroup struct {
  wMutex    *sync.Mutex
  working   map[TaskId]bool
  rMutex    *sync.Mutex
  ready     *list.List  
  dMutex    *sync.Mutex
  done      *list.List
  hMutex 		*sync.Mutex
  history		*list.List
}

func makeTaskIdGroup() *TaskIdGroup {
  return &TaskIdGroup{
    wMutex: &sync.Mutex{},
    working: make(map[TaskId]bool),
    rMutex: &sync.Mutex{},
    ready: list.New(),
    dMutex: &sync.Mutex{},
    done: list.New(), 
    hMutex: &sync.Mutex{},
    history: list.New()}
}

func (t *TaskIdGroup) ClearReady() *list.List {
  t.rMutex.Lock()
  defer t.rMutex.Unlock()
  tmp := t.ready
  t.ready = list.New()
  return tmp
}

func (t *TaskIdGroup) ClearWorking() map[TaskId]bool {
  t.wMutex.Lock()
  defer t.wMutex.Unlock()
  tmp := t.working
  t.working = make(map[TaskId]bool)
  return tmp
}

func (t *TaskIdGroup) ClearDone() *list.List {
  t.dMutex.Lock()
  defer t.dMutex.Unlock()
  tmp := t.done
  t.done = list.New()
  return tmp
}

func (t *TaskIdGroup) PushBackToReady(id TaskId) {
  t.rMutex.Lock()
  defer t.rMutex.Unlock()
  t.ready.PushBack(id)
}

func (t *TaskIdGroup) PushBackListToReady(l *list.List) {
  t.rMutex.Lock()
  defer t.rMutex.Unlock()
  t.ready.PushBackList(l)
}

func (t *TaskIdGroup) PushBackListToDone(l *list.List) {
	t.dMutex.Lock()
	defer t.dMutex.Unlock()
	t.done.PushBackList(l)
}

func (t *TaskIdGroup) RetriveReadyTaskID(taskID *TaskId) bool {
  t.rMutex.Lock()
  if t.ready.Len() !=0 {
    element := t.ready.Front()
    t.ready.Remove(element)
    t.rMutex.Unlock()
    *taskID = element.Value.(TaskId)
    return true
  }
  t.rMutex.Unlock()
  return false
}

func (t *TaskIdGroup) RemoveDoneTaskIds() []TaskId {
  t.dMutex.Lock()
  defer t.dMutex.Unlock()
  taskIds := make([]TaskId, 0)
  for e := t.done.Front(); e != nil; e = e.Next() {
    // do something with e.Value
    taskIds = append(taskIds, e.Value.(TaskId))
  }
  log.Printf("TaskIds %v  Be Removed From Done", taskIds)
  t.done = list.New()
  return taskIds
}

func (t *TaskIdGroup) AddToHistory(taskIds *list.List) {
  t.hMutex.Lock()
  defer t.hMutex.Unlock()
  t.history.PushBackList(taskIds)
  log.Printf("TaskIds %v  Add To History", *taskIds)
}

func (t *TaskIdGroup) RecoverFromHistory() *list.List {
  t.hMutex.Lock()
  defer t.hMutex.Unlock()
	li := t.history
	t.history = list.New()
  return li
}

func (t *TaskIdGroup) ToWorking(id TaskId) bool {
  t.wMutex.Lock()
  t.working[id] = true
  t.wMutex.Unlock()
  return true
}
func (t *TaskIdGroup) WorkingToDone(id TaskId) bool {
  t.wMutex.Lock()
  if _, ok := t.working[id]; !ok {
    t.wMutex.Unlock()
    return false
  }
  delete(t.working, id)
  t.wMutex.Unlock()
  t.dMutex.Lock()
  t.done.PushBack(id)
  t.dMutex.Unlock()
  return true
}

func (t *TaskIdGroup) WorkingToReady(id TaskId) bool {
  t.wMutex.Lock()
  if _, ok := t.working[id]; !ok {
    t.wMutex.Unlock()
    return false
  }
  delete(t.working, id)
  t.wMutex.Unlock()
  t.rMutex.Lock()
  t.ready.PushBack(id)
  t.rMutex.Unlock() 
  return true
}

func (t *TaskIdGroup) IsAllDone() bool {
  t.rMutex.Lock()
  t.wMutex.Lock()
  defer t.wMutex.Unlock()
  defer t.rMutex.Unlock()
  return  t.ready.Len() == 0 && len(t.working) == 0
}

func (t *TaskIdGroup) ReadySize() uint64 {
  t.rMutex.Lock()
  defer t.rMutex.Unlock()
  return uint64(t.ready.Len())
}

func (t *TaskIdGroup) WorkingSize() uint64 {
  t.wMutex.Lock()
  defer t.wMutex.Unlock()
  return uint64(len(t.working))
}

func (t *TaskIdGroup) DoneSize() uint64 {
  t.dMutex.Lock()
  defer t.dMutex.Unlock()
  return uint64(t.done.Len())
}

func (t *TaskIdGroup) HistorySize() uint64 {
	t.hMutex.Lock()
	defer t.hMutex.Unlock()
	return uint64(t.history.Len())
}