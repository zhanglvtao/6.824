package raft

func (rf *Raft) election() {
  rf.LOG.Println("+ New election")

  rf.refreshTimeOut()          // 1.Refresh election timer

  rf.statMu.Lock()
  oldTerm := rf.curTerm
  newTerm := oldTerm + 1
  rf.role = RaftRoleCandidate  // 2.Convert to condidate 
  rf.curTerm = newTerm         // 3.Increment oldTerm
  rf.voteFor = RaftId(rf.me)   // 4.Vote for itself
  rf.statMu.Unlock()

  rf.LOG.Printf("Increment curTerm from %v to %v", oldTerm, newTerm)
  lastCommittedLogIndex := rf.SyncGetCommitIndex()
  lastCommittedLogTerm := rf.SyncGetLogEntry(lastCommittedLogIndex).Term
  // 5.Send RPC
  args := RequestVoteArgs{
    CandidateTerm:         newTerm,
    CandidateId:           rf.id,
    LastCommittedLogIndex: lastCommittedLogIndex,
    LastCommittedLogTerm:  lastCommittedLogTerm}
  ch := make(chan *RequestVoteReply)
  count := 0
  for peer := range rf.peers {
    if peer == rf.me {
      continue
    }
    reply := RequestVoteReply{
      VoterTerm:   RaftTermInvalid,
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
      if reply.VoterTerm == 0 {
        rf.LOG.Printf("< Raft-%v rejected with network fail", reply.VoterId)
      } else {
        rf.LOG.Printf("< Raft-%v rejected with term %v", reply.VoterId, reply.VoterTerm)
      }
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
    rf.LOG.Printf("Abort election, term %v", curTerm)
    return
  }
  if granted < majority {
    rf.statMu.Lock()
    defer rf.statMu.Unlock()
    rf.LOG.Printf("Lose election, %v votes", granted)
    rf.role = RaftRoleFollower
    rf.voteFor = RaftIdInvalid
    return
  }

  rf.LOG.Printf("✅ Win election, %v votes. Term %v", granted, curTerm)

  rf.statMu.Lock()
  rf.role = RaftRoleLeader
  rf.statMu.Unlock()

  // Reinitial rf.nextIndex[] & start commit loop
  rf.InitNextIndex()
  go rf.appendLoop()
  go rf.heartbeatLoop()
  go rf.commitLoop()

  curLogIndex, _ := rf.SyncGetCurLogIndexTerm()
  rf.LOG.Printf("rf.curLogIndex(%v) / LogIndexInital(%v)", curLogIndex, LogIndexInitial)
  if curLogIndex != LogIndexInitial {
    noop := rf.appendNewEntry(CommandNoOp) 
    rf.matchIdxMu.Lock()
    rf.matchIndex[rf.me] = noop.Index
    rf.matchIdxMu.Unlock()
    rf.LOG.Printf("Submit NoopCommand after winning. Entry %v", noop)
  }
}