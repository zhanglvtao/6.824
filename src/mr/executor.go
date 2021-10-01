package mr 

type UnixTimeStamp  uint64
type ExecutorStatus uint8
type ExecutorId     uint64
const (
  INVALID_EXECUTOR_ID = 0
)
const (
  ExecutorStatusAlive  ExecutorStatus = iota
  ExecutorStatusDead
)
type Executor struct {
  id                  ExecutorId
  taskId              TaskId
  lastHeartbeatTime   UnixTimeStamp
  status              ExecutorStatus
}