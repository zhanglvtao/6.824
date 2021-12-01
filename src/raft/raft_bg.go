package raft
import (
  "time"
  "sort"
)

func (rf *Raft) appendLoop() {
  for !rf.killed() {
    _, curRole, _ := rf.SyncGetRaftStat()
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
    commitIndex := rf.SyncGetCommitIndex()
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
    // 1. Get snapshot of macthIndex (matchIndex increases monotonically)
    rf.matchIdxMu.RLock()
    peersNum := len(rf.peers)
    macthIndex := make([]LogIndex, peersNum)
    copy(macthIndex, rf.matchIndex) 
    rf.matchIdxMu.RUnlock()

    // 2. Sort the snapshot
    sort.Slice(macthIndex, func(i, j int) bool {
      return macthIndex[i] < macthIndex[j]
    })

    // 3. Find the N that marjority of server got replicated it
    mid := peersNum/2
    rf.matchIdxMu.RLock()
    index := rf.matchIndex[mid]
    rf.matchIdxMu.RUnlock() 

    entry := rf.SyncGetLogEntry(index)

    // 4. Set commitIndex = N
    curTerm, _, _ := rf.SyncGetRaftStat()
    if entry.Term != curTerm {
      rf.LOG.Printf("Abort update commitIndex, term dismatch. Entry term %v, current term %v", entry.Term, rf.curTerm)
      time.Sleep(500 * time.Millisecond)
      continue
    }

    if rf.SyncGetCommitIndex() >= entry.Index {
      time.Sleep(500 * time.Millisecond)
      continue
    }

    rf.LOG.Printf("Update commitIndex from %v to %v", rf.commitIndex, entry.Index)
    rf.SyncSetCommitIndex(entry.Index)

    time.Sleep(500 * time.Millisecond)
  }
}