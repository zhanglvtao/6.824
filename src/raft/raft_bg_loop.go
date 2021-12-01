package raft
import (
  "time"
  "sort"
)

func (rf *Raft) appendLoop() {
  for !rf.killed() {
    rf.statMu.Lock()
    curRole := rf.role
    rf.statMu.Unlock()
    if curRole != RaftRoleLeader {
      time.Sleep(50 * time.Millisecond)
      continue
    }
    // Append entry as leader
    rf.InLeader()
    time.Sleep(50 * time.Millisecond)
  }
}

// The electLoop go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// Single goroutine
func (rf *Raft) electLoop() {
  // init timer
  rf.refreshTimeOut()
  for rf.killed() == false {
    // Your code here to check if a leader election should
    // be started and to randomize sleeping time using
    //if rf.role == RaftRoleLeader {
    //  rf.LOG.Printf("- Leader Term %v", rf.curTerm)
    //  time.Sleep(rf.lastTimeOut)
    //  continue
    //}
    // Part 1. Check whether received append entries
    timeToTimeOut := rf.tickTimeOut()
    if timeToTimeOut > 0 {
      time.Sleep(timeToTimeOut)
      continue
    }

    // Part 2. incremental curTerm & vote itself & reset election timer && send RequestVote RPC
    rf.refreshTimeOut()
    go rf.InCandidate()
  }
}

func (rf *Raft) applyLoop() {
  for {
    rf.commitIdxMu.Lock()
    commitIndex := rf.commitIndex
    rf.commitIdxMu.Unlock()
    rf.lastApplyMu.Lock()
    for commitIndex > rf.lastApplied {
      rf.lastApplied++
      rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logEntries[rf.lastApplied], CommandIndex: int(rf.lastApplied)}
      rf.LOG.Printf("Apply command to application index %v - command %v", rf.lastApplied, rf.logEntries[rf.lastApplied])
    }
    rf.LOG.Printf("Catch up commit index %v", commitIndex)
    rf.lastApplyMu.Unlock()
    time.Sleep(500 * time.Millisecond)
  }
}

func (rf *Raft) commitLoop() {
  for {
    rf.nextIdxMu.Lock()
    nextIndex := make([]LogIndex, len(rf.nextIndex))
    copy(nextIndex, rf.nextIndex) 
    rf.nextIdxMu.Unlock()
    sort.Slice(nextIndex, func(i, j int) bool {
      return nextIndex[i] < nextIndex[j]
    })
    mid := len(nextIndex)/2
    rf.entriesMu.Lock()
    index := rf.nextIndex[mid]
    entry := rf.logEntries[index - 1]
    rf.entriesMu.Unlock()
    rf.statMu.Lock()
    curTerm := rf.curTerm
    rf.statMu.Unlock()
    if entry.Term != curTerm {
      rf.LOG.Printf("Abort update commitIndex, term dismatch. Entry term %v, current term %v", entry.Term, rf.curTerm)
      time.Sleep(500 * time.Millisecond)
      continue
    }
    rf.commitIdxMu.Lock()
    if rf.commitIndex >= entry.Index {
      time.Sleep(500 * time.Millisecond)
      rf.commitIdxMu.Unlock()
      continue
    }
    rf.LOG.Printf("Update commitIndex from %v to %v", rf.commitIndex, nextIndex[mid])
    rf.commitIndex = nextIndex[mid]
    rf.commitIdxMu.Unlock()
    time.Sleep(500 * time.Millisecond)
  }
}