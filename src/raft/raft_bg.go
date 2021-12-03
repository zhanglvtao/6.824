package raft
import (
  "time"
  "sort"
)

func (rf *Raft) appendLoop() {
  for !rf.killed() {
    _, curRole, _ := rf.SyncGetRaftStat()
    if curRole != RaftRoleLeader {
      rf.LOG.Println("Append loop: Not leader, exit goroutine")
      return
    }
    // Append entry as leader
    curTerm, _, _ := rf.SyncGetRaftStat()
    for peer := range rf.peers {
      if peer == rf.me {
        continue
      }
      var nextLogIndex  LogIndex
      var entry         LogEntry
      var prevLogIndex  LogIndex
      var prevLogTerm   Term
      var leaderCommit  LogIndex

      len, _ := rf.SyncGetLogEntryLenCap()
      nextLogIndex = rf.SyncGetNextIndex(RaftId(peer)) 
      
      if len == 1 {
        time.Sleep(SleepAppend)
        rf.LOG.Printf("🚩 No entry to apppend to follower")
        continue
      }

      if nextLogIndex == LogIndex(len) {
        //rf.LOG.Printf("Choose the last entry to send %v", rf.SyncGetLogEntry(nextLogIndex))
        time.Sleep(SleepAppend)
        rf.LOG.Printf("🚩 Every entry has append to follower")
        continue
      }
  
      prevLogIndex = nextLogIndex - 1
      prevLogTerm = rf.SyncGetLogEntry(prevLogIndex).Term
      leaderCommit = rf.SyncGetCommitIndex() 

      entry = rf.SyncGetLogEntry(nextLogIndex)
      
      args := AppendEntriesArgs{
        LeaderTerm:     curTerm,
        LeaderId:       rf.id,
        PrevLogIndex:   prevLogIndex,
        PrevLogTerm:    prevLogTerm,
        Entry:      	  entry,
        LeaderCommit:   leaderCommit}
      reply := AppendEntriesReply{}
      go rf.ChanSendAppendEntries(nil, peer, &args, &reply)
    }
    time.Sleep(SleepAppend)
  }
}

func(rf *Raft) heartbeatLoop() {
  for !rf.killed() {
    _, curRole, _ := rf.SyncGetRaftStat()
    if curRole != RaftRoleLeader {
      rf.LOG.Println("Heartbeat loop: Not leader, exit goroutine")
      return
    }
    curTerm, _, _ := rf.SyncGetRaftStat()
    ch := make(chan *AppendEntriesReply)
    count := len(rf.peers)
    for peer := range rf.peers {
      if peer == rf.me {
        continue
      }
      var nextLogIndex  LogIndex
      var entry         LogEntry
      var prevLogIndex  LogIndex
      var prevLogTerm   Term
      var leaderCommit  LogIndex
  
      nextLogIndex = rf.SyncGetNextIndex(RaftId(peer)) 
      prevLogIndex = nextLogIndex - 1
      prevLogTerm = rf.SyncGetLogEntry(prevLogIndex).Term
      leaderCommit = rf.SyncGetCommitIndex()
  
      entry = LogEntry{Term: curTerm, Index: LogIndexInitial, Command: CommandHeartbeat}

      args := AppendEntriesArgs{
        LeaderTerm:     curTerm,
        LeaderId:       rf.id,
        PrevLogIndex:   prevLogIndex,
        PrevLogTerm:    prevLogTerm,
        Entry:      	  entry,
        LeaderCommit:   leaderCommit}
      reply := AppendEntriesReply{}
      go rf.ChanSendAppendEntries(ch, peer, &args, &reply)
    }
    majority := count/2 + 1
    received := 1
    for range ch {
      count--
      received++
      if received >= majority {
        rf.LOG.Printf("❤️ Still LEADER, received %v/%v append reply", received, len(rf.peers))
        break
      }
      if count == 0 {
        close(ch)
        rf.LOG.Printf("Close append channel")
        break
      }
    }
    if received >= majority {
      rf.refreshTimeOut()
      time.Sleep(SleepHeartbeat)
      continue
    }
  
    rf.statMu.Lock() 
    if rf.role == RaftRoleLeader && rf.curTerm == curTerm {
      rf.role = RaftRoleFollower
    }
    rf.statMu.Unlock()
  
    rf.LOG.Printf("Leader => Follower, not receive enought append reply")
    time.Sleep(SleepHeartbeat)
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
    go rf.election()
  }
}

// This loop check rf.commitIndex, then apply command the state machine, then update rf.lastApplied
func (rf *Raft) applyLoop() {
  for {
    var commitIndex LogIndex
    var lastApplied LogIndex

    commitIndex = rf.SyncGetCommitIndex()
    lastApplied = rf.lastApplied
    rf.LOG.Printf("🥶 Apply loop. CommitIndex(%v) LastApplied (%v)", commitIndex, lastApplied)

    for commitIndex > lastApplied {
      lastApplied++
      curLogIndex, _ := rf.SyncGetCurLogIndexTerm()
      if lastApplied > curLogIndex {
        rf.LOG.Printf("Waiting append loop to append enough entry. Now we have index %v, need to %v", curLogIndex, lastApplied)
        break
      }
      applyEntry := rf.SyncGetLogEntry(lastApplied)
      rf.applyCh <- ApplyMsg{CommandValid: true, Command: applyEntry.Command, CommandIndex: int(lastApplied)}
      rf.lastApplied = lastApplied
      rf.LOG.Printf("🐷 Apply to application. Entry %v", applyEntry)
    }

    time.Sleep(500 * time.Millisecond)
  }
}

// Note: Only for leader to call
// This loop check rf.matchIndex[] to find N, then update rf.commitIndex to N
func (rf *Raft) commitLoop() {
  for {
    rf.LOG.Printf("🐤 Commit loop. CommitIndex(%v)", rf.SyncGetCommitIndex())
    // rf.LOG.Printf("LogEntries %v", rf.logEntries)
    // 1. Get snapshot of macthIndex (matchIndex increases monotonically)
    rf.matchIdxMu.RLock()
    peersNum := len(rf.peers)
    matchIndex := make([]LogIndex, peersNum)
    copy(matchIndex, rf.matchIndex) 
    rf.matchIdxMu.RUnlock()

    // 2. Sort the snapshot
    sort.Slice(matchIndex, func(i, j int) bool {
      return matchIndex[i] < matchIndex[j]
    })
    //rf.LOG.Printf("Matches %v", rf.matchIndex)
    //rf.LOG.Printf("NextIndex %v", rf.nextIndex)
    // 3. Find the N that marjority of server got replicated it
    mid := peersNum/2
    rf.matchIdxMu.RLock()
    index := matchIndex[mid]
    rf.matchIdxMu.RUnlock() 

    entry := rf.SyncGetLogEntry(index)

    // 4. Set commitIndex = N
    curTerm, _, _ := rf.SyncGetRaftStat()
    if entry.Term != curTerm {
      rf.LOG.Printf("Cancel update commitIndex, term dismatch. Entry term %v, current term %v", entry.Term, curTerm)
      time.Sleep(SleepCommit)
      continue
    }

    if rf.SyncGetCommitIndex() >= entry.Index {
      rf.LOG.Printf("No need to set. CommitIndex(%v) / Found-N(%v)", rf.SyncGetCommitIndex(), entry.Index)
      time.Sleep(SleepCommit)
      continue
    }

    rf.SyncSetCommitIndex(entry.Index)
    time.Sleep(SleepCommit)
  }
}