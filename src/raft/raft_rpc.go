package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
  // Your data here (2A, 2B).
  CandidateTerm         Term
  CandidateId           RaftId
  LastCommittedLogIndex LogIndex
  LastCommittedLogTerm  Term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
  // Your data here (2A).
  VoterTerm   Term
  VoteGranted bool
  VoterId     RaftId
}

type AppendEntriesArgs struct {
  LeaderTerm   Term
  LeaderId     RaftId
  PrevLogIndex LogIndex // immediately preceding new one (Entry field below)
  PrevLogTerm  Term     // immediately preceding new one (Entry field below)
  Entry        LogEntry // The new entry for the follower
  LeaderCommit LogIndex // Leader's commit index
}

type AppendEntriesReply struct {
  FollowId   RaftId
  FollowTerm Term
  Success    bool
}

func (rf *Raft) ChanRequestVote(ch chan *RequestVoteReply, server int, args *RequestVoteArgs, reply *RequestVoteReply) {
  ok := rf.sendRequestVote(server, args, reply)
  if !ok {
    rf.LOG.Printf("- RPC::RequestVote(SendTo %v) Fail", reply.VoterId)
  }
  ch <- reply
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  // Your code here (2A, 2B).
  // Condition 1: Term dismatch OR log index dismatch
  curTerm, _, _ := rf.SyncGetRaftStat()
  commitIndex := rf.SyncGetCommitIndex()
  commitTerm := rf.SyncGetLogEntry(commitIndex).Term
  reply.VoterTerm = curTerm
  reply.VoterId = rf.id
  reply.VoteGranted = false

  if args.CandidateTerm <= reply.VoterTerm {
    rf.LOG.Printf("> Reject vote candidate %v, term %v left behind me %v", args.CandidateId, args.CandidateTerm, reply.VoterTerm)
    return
  }
  rf.LOG.Printf("🟢 candidate LastCommittedLogIndex(%v) / my commitIndex(%v), LastCommittedLogTerm(%v) / commitTerm(%v)", args.LastCommittedLogIndex, commitIndex, args.LastCommittedLogTerm, commitTerm)
  if args.LastCommittedLogIndex < commitIndex && args.LastCommittedLogTerm <= commitTerm {
    rf.LOG.Printf("> Reject vote candidate %v, candidate last committed entry term(%v) index(%v). My term(%v) index(%v)", args.CandidateId, args.LastCommittedLogIndex, args.LastCommittedLogTerm, commitTerm, commitIndex)
    return
  }

  // Condition 2: Vote
  rf.refreshTimeOut()

  rf.statMu.Lock()
  defer rf.statMu.Unlock()
  oldTerm := rf.curTerm

  rf.voteFor = args.CandidateId
  rf.curTerm = args.CandidateTerm
  rf.role = RaftRoleFollower

  reply.VoteGranted = true
  reply.VoterTerm = rf.curTerm
  rf.LOG.Printf("> Vote for %v, term %v => %v", args.CandidateId, oldTerm, rf.curTerm)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  return ok
}

func (rf *Raft) ChanSendAppendEntries(ch chan *AppendEntriesReply, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
  ok := rf.sendAppendEntries(server, args, reply)
  if ch != nil {
    ch <- reply
  }
  if !ok {
    rf.LOG.Printf("::SendAppendEntires(TO %v) Fail with network error", reply.FollowId)
    return
  }
  curTerm, _, _ := rf.SyncGetRaftStat()
  if reply.FollowTerm > curTerm {
    rf.statMu.Lock()
    defer rf.statMu.Unlock()
    rf.role = RaftRoleFollower
    rf.curTerm = reply.FollowTerm
    rf.LOG.Printf("::SendAppendEntries(TO %v) Encounter greater term: Leader => %v with term %v", server, rf.role, rf.curTerm)
    return
  }

  rf.nextIdxMu.Lock()
  defer rf.nextIdxMu.Unlock()
  rf.matchIdxMu.Lock()
  defer rf.matchIdxMu.Unlock()
  if reply.Success {
    if args.Entry.Command == CommandHeartbeat {
      //rf.LOG.Printf("::SendAppendEntries(TO %v) Success. Heartbeat command. Entry %v", server, args.Entry)
      return
    }
    rf.nextIndex[server] = args.Entry.Index + 1
    rf.matchIndex[server] = args.Entry.Index
    rf.LOG.Printf("::SendAppendEntries(TO %v) Success. Now match %v, next %v", server, rf.matchIndex[server], rf.nextIndex[server])
    return
  }
  rf.nextIndex[server] = LogIndex(Max(1, int(args.Entry.Index) - 1))
  rf.matchIndex[server] = 0
  rf.LOG.Printf("::SendAppendEntries(TO %v) Fail. Now match %v, next %v", server, rf.matchIndex[server], rf.nextIndex[server])
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
  return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// As receiver
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  reply.Success = false
  reply.FollowId = rf.id

  rf.refreshTimeOut()
  // Wrong leader, if term dismatch
  curTerm, _, _ := rf.SyncGetRaftStat()
  reply.FollowTerm = curTerm
  if args.LeaderTerm < reply.FollowTerm {
    rf.LOG.Printf("+::AppendEntries Raft-%v should convert to follower", args.LeaderId) //. Term dismatch follower %v, leader %v", rf.curTerm, args.LeaderTerm)
    return
  }

  // Legal Leader. Heartbeat.
  var oldCommitIndex LogIndex
  var newCommitIndex LogIndex
  if args.LeaderCommit > args.Entry.Index && args.Entry.Index != LogIndexInitial {
    newCommitIndex = args.Entry.Index
  } else {
    newCommitIndex = args.LeaderCommit
  }
  oldCommitIndex = rf.SyncSetCommitIndex(newCommitIndex)
  rf.LOG.Printf("__::AppendEntries rf.commitIndex %v -> min(leaderCommit %v, entry.Index %v)", oldCommitIndex, args.LeaderCommit, args.Entry.Index)

  rf.statMu.Lock()
  rf.role = RaftRoleFollower
  rf.curTerm = args.LeaderTerm
  if args.Entry.Command == CommandHeartbeat {
    reply.Success = true
    //rf.LOG.Printf("+::AppendEntries as heartbeat")
    rf.statMu.Unlock()
    return
  }
  rf.statMu.Unlock()

  rf.logEntriesMu.Lock()
  rf.curLogMu.Lock()
  defer rf.curLogMu.Unlock()
  defer rf.logEntriesMu.Unlock()
  // Legal leader. Append entry.
  // Prev log not exist
  if len(rf.logEntries) <= int(args.PrevLogIndex) {
    reply.Success = false
    rf.LOG.Printf("+::AppendEntries not exits PrevLogIndex %v, len(rf.logEntries) = %v", args.PrevLogIndex, len(rf.logEntries))
    return
  }
  // Prev log dismatch
  if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
    reply.Success = false
    rf.logEntries = rf.logEntries[:args.PrevLogIndex]
    rf.LOG.Printf("+::AppendEntries delete all log from index %v", args.PrevLogIndex)
    return
  }
  // Entry already exists, index and term match.
  reply.Success = true
  if len(rf.logEntries) == int(args.Entry.Index) + 1 && rf.logEntries[args.Entry.Index].Term == args.Entry.Term {
    rf.LOG.Printf("+::AppendEntries append entry already exists. Entry %v", args.Entry)
    return
  }
  // Entry supposed to be add.
  rf.LOG.Printf("+::AppendEntries append --")
  reply.Success = true
  rf.appendOldEntry(&args.Entry)
}
