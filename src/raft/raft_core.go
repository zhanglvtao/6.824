package raft

func (rf *Raft) InCandidate() {
  rf.LOG.Println("+ New election")

  rf.refreshTimeOut() // 1.Refresh election timer

  rf.statMu.Lock()
  oldTerm := rf.curTerm
  newTerm := oldTerm + 1
  rf.role = RaftRoleCandidate // 2.Convert to condidate 
  rf.curTerm = newTerm // 3.Increment oldTerm
  rf.voteFor = RaftId(rf.me)   // 4.Vote for itself
  rf.statMu.Unlock()

  rf.LOG.Printf("Increment curTerm from %v to %v", oldTerm, newTerm)

  // 5.Send RPC
  args := RequestVoteArgs{
    CandidateTerm:         newTerm,
    CandidateId:           rf.id,
    CandidateLastLogIndex: rf.curLogIndex,
    CandidateLastLogTerm:  rf.curLogTerm}
  ch := make(chan *RequestVoteReply)
  count := 0
  for peer := range rf.peers {
    if peer == rf.me {
      continue
    }
    reply := RequestVoteReply{
      VoterTerm:   InvalidRaftTerm,
      VoterId:     RaftId(peer),
      VoteGranted: false}
    count++
    go rf.ChanRequestVote(ch, peer, &args, &reply)

    rf.LOG.Printf("ChRequestVote to %v", peer)
  }
  majority := count/2 + 1
  granted := 1
  for reply := range ch {
    count--
    if reply.VoteGranted {
      granted++
      rf.LOG.Printf("< Raft-%v granted with term %v", reply.VoterId, reply.VoterTerm)
    } else {
      rf.LOG.Printf("< Raft-%v rejected with term %v", reply.VoterId, reply.VoterTerm)
    }
    // If received majority of peers' granted vote. Not wait within the loop anymore.
    if granted >= majority {
      rf.LOG.Println("Recevied majortiy peers' grant. Break loop")
      break
    }
    // If received all votes
    if count == 0 {
      close(ch)
      rf.LOG.Printf("Received all peers' vote. Close channel")
      break
    }
  }

  curTerm, _, _ := rf.SyncGetRaftStat()
  if curTerm != oldTerm + 1 {
    rf.LOG.Printf("Abort election, term %v", rf.curTerm)
    return
  }
  if granted < majority {
    rf.statMu.Lock()
    defer rf.statMu.Unlock()
    rf.LOG.Printf("Lose election, %v votes", granted)
    rf.role = RaftRoleFollower
    rf.voteFor = InvalidRaftId
    return
  }

  rf.LOG.Printf("Win election, %v votes. Convert from %v to Leader", granted, rf.role)

  rf.statMu.Lock()
  rf.role = RaftRoleLeader
  rf.statMu.Unlock()

  // After just as leader
  rf.InitNextIndex()
  newLogIndex := rf.curLogIndex + 1
  noOp := &LogEntry{ Term: curTerm, Index: newLogIndex, Command: "NoOperation"}
  rf.commitCh <- noOp
  rf.curLogIndex = newLogIndex
  rf.LOG.Printf("Submit a noOp after winning election")
}

func (rf *Raft) InLeader() {
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

		nextLogIndex = rf.SyncGetNextIndex(RaftId(peer)) 
    prevLogIndex = nextLogIndex - 1
		prevLogTerm = rf.SyncGetLogEntry(prevLogIndex).Term
		len, _ := rf.SyncGetLogEntryLenCap()

    if nextLogIndex == LogIndex(len) {
      entry = LogEntry{Term: curTerm, Index: InvalidLogIndex, Command: nil}
    } else {
			entry = rf.SyncGetLogEntry(nextLogIndex)
    }
    args := AppendEntriesArgs{
      LeaderTerm:     curTerm,
      LeaderId:       rf.id,
      PrevLogIndex:   prevLogIndex,
      PrevLogTerm:    prevLogTerm,
      Entry:      	  entry,
      LeaderCommit:   rf.commitIndex}
    reply := AppendEntriesReply{}
    go rf.ChanSendAppendEntries(ch, peer, &args, &reply)
  }
  majority := count/2 + 1
  received := 1
  for range ch {
    count--
    received++
    if received >= majority {
      rf.LOG.Printf("Continue be Leader, received %v/%v append reply", received, len(rf.peers))
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
    return
  }

  rf.statMu.Lock() 
  if rf.role == RaftRoleLeader && rf.curTerm == curTerm {
    rf.role = RaftRoleFollower
  }
  rf.statMu.Unlock()

  rf.LOG.Printf("Leader => Follower, not receive enought append reply")
}
