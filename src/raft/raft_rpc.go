package raft

import "time"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
  // Your data here (2A, 2B).
  CandidateTerm         Term
  CandidateId           RaftId
  CandidateLastLogIndex LogIndex
  CandidateLastLogTerm  Term
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
  PreLogIndex  LogIndex
  PreLogTerm   Term
  Entries[]    Entry
  LeaderCommit LogIndex // leader's commit index
}

type AppendEntriesReply struct {
  FollowId     RaftId
  FollowTerm  Term
  Success     bool
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
  trial := 0
  for {
    if rf.statMu.TryLock() {
      break;
    }
    trial++;
    if trial == 10 {
      rf.LOG.Printf("> Reject vote candidate %v, unable to acquire statMu", args.CandidateId)
    }
    time.Sleep(5 * time.Millisecond)
  }
  //if !rf.statMu.TryLock() {
  //  rf.LOG.Printf("> Reject vote candidate %v, unable to acquire statMu", args.CandidateId)
  //  return
  //}
  defer rf.statMu.Unlock() 
  reply.VoterTerm = rf.curTerm
  reply.VoterId = rf.id
  reply.VoteGranted = false
  if args.CandidateTerm <= rf.curTerm {
    rf.LOG.Printf("> Reject vote candidate %v, term %v left behind me %v", args.CandidateId, args.CandidateTerm, rf.curTerm)
    return
  }
  if args.CandidateLastLogIndex < rf.curLogIndex {
    rf.LOG.Printf("> Reject vote candidate %v, log-index %v left behind of %v", args.CandidateId, args.CandidateLastLogIndex, rf.curLogIndex)
    return
  }
  //if rf.voteFor != InvalidRaftId {
  //  rf.LOG.Printf("> Reject vote, has granted to %v", rf.voteFor)
  //}
  // Condition 2: Vote
  rf.voteFor = args.CandidateId
  old := rf.curTerm
  rf.curTerm = args.CandidateTerm
  rf.role = RaftRoleFollower
  reply.VoteGranted = true
  reply.VoterTerm = old
  rf.LOG.Printf("> Vote for %v, term %v => %v", rf.voteFor, old, rf.curTerm)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  return ok
}

func (rf *Raft) ChanSendAppendEntries(ch chan *AppendEntriesReply, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
  ok := rf.sendAppendEntries(server, args, reply)
  if !ok {
    rf.LOG.Printf("- RPC::SendAppendEntires(Send To %v) Fail", reply.FollowTerm)
  }
  ch <- reply
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
  ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
  if reply.FollowTerm <= rf.curLogTerm {
    return ok
  }
  rf.statMu.Lock()
  defer rf.statMu.Unlock()
  rf.role = RaftRoleFollower
  rf.LOG.Printf("sendAppendEntries: Leader => %v", rf.role)
  return ok
}

// As receiver
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  reply.Success = false
  reply.FollowTerm = rf.curLogTerm
  reply.FollowId = rf.id
  //if args.LeaderTerm < rf.curTerm {
  //  rf.LOG.Printf("+ RPC::AppendEntries Raft-%v should convert to follower", args.LeaderId)//. Term dismatch follower %v, leader %v", rf.curTerm, args.LeaderTerm)
  //  return
  //}
  rf.refreshTimeOut()
  if !rf.statMu.TryLock() {
    return
  }
  defer rf.statMu.Unlock()
  if args.LeaderTerm < rf.curTerm {
    reply.FollowTerm = rf.curTerm
    rf.LOG.Printf("+ RPC::AppendEntries Raft-%v should convert to follower", args.LeaderId)//. Term dismatch follower %v, leader %v", rf.curTerm, args.LeaderTerm)
    return
  }
  reply.Success = true
  reply.FollowTerm = rf.curLogTerm
  rf.role = RaftRoleFollower
  rf.LOG.Printf("+ RPC::AppendEntries")// Refresh lastTickTime %v, lastTimeOut %v", rf.lastTickTime, rf.lastTimeOut)
}