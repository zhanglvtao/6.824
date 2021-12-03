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

const SleepElection         time.Duration = time.Millisecond * 100
const SleepHeartbeat        time.Duration = time.Millisecond * 50
const SleepAppend           time.Duration = time.Millisecond * 100
const SleepCommit           time.Duration = time.Millisecond * 100

const CommandHeartbeat string = "Heartbeat"
const CommandNoOp      string = "Noop"
const CommandInitial   string = "Init"