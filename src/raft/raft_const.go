package raft
import(
  "time"
)

const (
  RaftRoleLeader    RaftRole = "Leader"
  RaftRoleCandidate RaftRole = "Candidate"
  RaftRoleFollower  RaftRole = "Follower"
)

const RaftIdInvalid 	    RaftId = -1
const RaftTermInvalid     Term = 0
const RaftTermInitial     Term = 1        // Term start from 1
const LogIndexInitial  	  LogIndex = 0    // Log index start from 0

const (
  SleepElection         time.Duration = time.Millisecond * 100
  SleepHeartbeat        time.Duration = time.Millisecond * 50
  SleepAppend           time.Duration = time.Millisecond * 100
  SleepCommit           time.Duration = time.Millisecond * 100
)

const (
  CommandHeartbeat      string = "Heartbeat"
  CommandNoOp           string = "Noop"
  CommandInitial        string = "Init"
)

const (
  Sync      bool = true
  NonSync   bool = false
)

const AppendEntriesSize int = 50