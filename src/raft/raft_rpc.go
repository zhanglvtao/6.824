package raft

import (
	"time"
)

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
  LeaderTerm    Term
  LeaderId      RaftId
  PrevLogIndex  LogIndex // immediately preceding new one (Entry field below)
  PrevLogTerm   Term     // immediately preceding new one (Entry field below)
  Entry         LogEntry // The new entry for the follower
  LeaderCommit  LogIndex // Leader's commit index
}

type AppendEntriesReply struct {
  FollowId    RaftId
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
      rf.LOG.Printf("> Reject vote candidate %v, statMu busy", args.CandidateId)
    }
    time.Sleep(5 * time.Millisecond)
  }

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

  // Condition 2: Vote
  rf.refreshTimeOut()
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
  ch <- reply
  if !ok {
    rf.LOG.Printf("::SendAppendEntires(TO %v) Fail with network error", reply.FollowId)
    return
  }
  rf.statMu.Lock()
  defer rf.statMu.Unlock()
  if reply.FollowTerm > rf.curTerm {
    rf.role = RaftRoleFollower
    rf.curTerm = reply.FollowTerm
    rf.LOG.Printf("::SendAppendEntries(TO %v) Encounter greater term: Leader => %v with term %v", server, rf.role, rf.curTerm)
    return
  }

  if reply.Success {
    if args.Entry.Index == InvalidLogIndex {
      rf.LOG.Printf("::SendAppendEntries(TO %v) Ok. As heartbeat.", server)
      return
    }
    rf.nextIndex[server] = rf.nextIndex[server] + 1
    rf.matchIndex[server] = rf.nextIndex[server] - 1
    rf.LOG.Printf("::SendAppendEntries(TO %v) Ok. Now match %v, next %v", server, rf.matchIndex[server], rf.nextIndex[server])
    return
  }
  rf.nextIndex[server] = rf.nextIndex[server] - 1
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

  // Return false if lock busy
  rf.refreshTimeOut()
  if !rf.statMu.TryLock() {
    return
  }
  // defer rf.statMu.Unlock()

  // Return false if term dismatch
  reply.FollowTerm = rf.curTerm
  if args.LeaderTerm < rf.curTerm {
    rf.LOG.Printf("+::AppendEntries Raft-%v should convert to follower", args.LeaderId)//. Term dismatch follower %v, leader %v", rf.curTerm, args.LeaderTerm)
    rf.statMu.Unlock()
    return
  }
  rf.role = RaftRoleFollower
  rf.curTerm = args.LeaderTerm
  // Just heartbeat, doesn't append any log
  if args.Entry.Index == InvalidLogIndex {
    reply.Success = true
    rf.LOG.Printf("+::AppendEntries as heartbeat")
    rf.statMu.Unlock()
    return
  } 
  rf.statMu.Unlock()
  // Prev log dismatch
  if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
    reply.Success = false
    rf.logEntries = rf.logEntries[:args.PrevLogIndex - 1]
    rf.LOG.Printf("+::AppendEntries delete all log from index %v", args.PrevLogIndex - 1)
    return
  }

  // Entry already exists
  reply.Success = true
  if rf.logEntries[args.Entry.Index].Term == args.Entry.Term {
    rf.LOG.Printf("+::AppendEntries append entry already exists. Term %v - index %v", args.Entry.Term, args.Entry.Index)
    return
  }
  // Entry not exists
  rf.logEntries = rf.logEntries[:args.Entry.Index - 1]
  rf.AppendLogEntry(&args.Entry)
  rf.LOG.Printf("+::AppendEntries append new entry. Term %v - index %v", args.Entry.Term, args.Entry.Index)

  rf.commitIdxMu.Lock()
  defer rf.commitIdxMu.Unlock()
  oldCommitIndex := rf.commitIndex
  if args.LeaderCommit > args.Entry.Index {
    rf.commitIndex = args.Entry.Index
  } else {
    rf.commitIndex = args.LeaderCommit
  }
  rf.LOG.Printf("_::AppendEntries update commitIndex from %v to %v", oldCommitIndex, rf.commitIndex)
}