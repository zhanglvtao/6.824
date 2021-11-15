package raft

import (
	"fmt"

)

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
  Entries      []LogEntry // The new entry for the follower
  LeaderCommit LogIndex // Leader's commit index
  MatchedIndex LogIndex // The follower matched index, follower accord it to apply log
}

func(ape * AppendEntriesArgs) String() string {
  ret := fmt.Sprintf(
    "LeaderId(%v), LeaderTerm(%v), PrevLogIndex(%v), PrevLogTerm(%v), Entry(%v), LeaderCommit(%v)", 
    ape.LeaderId, ape.LeaderTerm, ape.PrevLogIndex, ape.PrevLogTerm, ape.Entries, ape.LeaderCommit)
  return ret
}
type AppendEntriesReply struct {
  FollowId                RaftId
  FollowTerm              Term
  LastCommitIndex         LogIndex
  LastCommitTerm          Term
  Success                 bool
  // If prev log dismatch, then this field will be the first entry index in the AE.PrevLogTerm.Term
  // If the entry not exits then it equals to 0
  SuggestNextIndex        LogIndex
}

func (rf *Raft) ChanRequestVote(ch chan *RequestVoteReply, server int, args *RequestVoteArgs, reply *RequestVoteReply) {
  ok := rf.sendRequestVote(server, args, reply)
  if !ok {
    rf.LOG.Printf("- RPC::RequestVote(SendTo %v) Fail", reply.VoterId)
  }
  curTerm, _, _ := rf.SyncGetRaftStat()
  if curTerm < reply.VoterTerm {
    rf.statMu.Lock()
    rf.curTerm = reply.VoterTerm
    rf.role = RaftRoleFollower
    rf.statMu.Unlock()
  }
  ch <- reply
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  // Refresh election timer
  rf.refreshTimeOut()

  // Initial reply
  curTerm, _, _ := rf.SyncGetRaftStat()
  commitIndex := rf.SyncGetCommitIndex()
  commitTerm := rf.SyncGetLogEntry(commitIndex).Term
  reply.VoterTerm = curTerm
  reply.VoterId = rf.id
  reply.VoteGranted = false

  // Term dismatch
  if args.CandidateTerm <= reply.VoterTerm {
    rf.LOG.Printf("> Reject vote candidate %v, term %v left behind me %v", args.CandidateId, args.CandidateTerm, reply.VoterTerm)
    return
  }

  // Encount greater term
  rf.statMu.Lock()
  oldTerm := rf.curTerm
  rf.curTerm = args.CandidateTerm
  rf.role = RaftRoleFollower
  rf.statMu.Unlock()
  
  //rf.LOG.Printf("🟢 candidate LastCommittedLogIndex(%v) / my commitIndex(%v), LastCommittedLogTerm(%v) / commitTerm(%v)", args.LastCommittedLogIndex, commitIndex, args.LastCommittedLogTerm, commitTerm)
  if args.LastCommittedLogIndex < commitIndex && args.LastCommittedLogTerm <= commitTerm {
    rf.LOG.Printf("> Reject vote candidate %v.I'm more up to date then candidate. \nCandidateLastCommitTerm(%v)Index(%v). MyLastCommitTerm(%v)Index(%v)", args.CandidateId, args.LastCommittedLogIndex, args.LastCommittedLogTerm, commitTerm, commitIndex)
    return
  }

  // Grant the vote
  rf.statMu.Lock()
  rf.voteFor = args.CandidateId
  rf.curTerm = args.CandidateTerm
  rf.role = RaftRoleFollower
  rf.statMu.Unlock()

  reply.VoteGranted = true
  reply.VoterTerm = rf.curTerm
  rf.LOG.Printf("> Vote %v -> %v, term %v => %v", rf.me, args.CandidateId, oldTerm, rf.curTerm)
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
    commitIndex := rf.SyncGetCommitIndex()
    if reply.LastCommitIndex < commitIndex {
      rf.LOG.Printf("Follower-%v term %v commitIndex %v. Leader-%v term %v commitIndex %v", reply.FollowId, reply.FollowTerm, reply.LastCommitIndex, rf.me, curTerm, commitIndex)
    }
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
    if args.Entries[0].Command == CommandHeartbeat {
      //rf.LOG.Printf("::SendAppendEntries(TO %v) Success. Heartbeat command. Entry %v", server, args.Entry)
      return
    }
    lastEntryAppend := args.Entries[len(args.Entries) - 1]
    rf.nextIndex[server] = lastEntryAppend.Index + 1
    rf.matchIndex[server] = lastEntryAppend.Index
    rf.LOG.Printf("::SendAppendEntries(TO %v) Success. Now match %v, next %v", server, rf.matchIndex[server], rf.nextIndex[server])
    return
  }
  rf.nextIndex[server] = reply.SuggestNextIndex
  rf.matchIndex[server] = 0
  rf.LOG.Printf("::SendAppendEntries(TO %v) Fail. Now match %v, next %v", server, rf.matchIndex[server], rf.nextIndex[server])
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
  return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  // Initial
  rf.LOG.Printf("Receive AE. Args: %v", args)
  reply.Success = true
  reply.FollowId = rf.id
  reply.LastCommitIndex = rf.SyncGetCommitIndex()
  reply.FollowTerm, _, _ = rf.SyncGetRaftStat()
  reply.SuggestNextIndex = 1
  firstNewEntry := args.Entries[0]
  lastNewEntry := args.Entries[len(args.Entries) - 1]

  // Refresh election timer
  rf.refreshTimeOut()

  // Term dismatch
  if args.LeaderTerm < reply.FollowTerm {
    rf.LOG.Printf("<::AppendEntries Raft-%v should convert to follower", args.LeaderId) //. Term dismatch follower %v, leader %v", rf.curTerm, args.LeaderTerm)
    reply.Success = false
    return
  }
 
  // Update rf.commitIndex
  var newCommitIndex LogIndex
  if args.LeaderCommit > lastNewEntry.Index && lastNewEntry.Index != LogIndexInitial {
    newCommitIndex = lastNewEntry.Index
    rf.LOG.Printf("<::AppendEntries. Set my CommitIndex to Entry.Index %v.", newCommitIndex)
  } else {
    newCommitIndex = args.LeaderCommit
    rf.LOG.Printf("<::AppendEntries. Set my CommitIndex to LeaderCommitIndex %v.", newCommitIndex)
  }
  rf.SyncSetCommitIndex(newCommitIndex)

  // Update matchedIndex
  rf.SyncSetMatchedIndex(args.MatchedIndex)

  // Heartbeat
  if lastNewEntry.Command == CommandHeartbeat {
    rf.statMu.Lock()
    rf.role = RaftRoleFollower
    rf.curTerm = args.LeaderTerm
    rf.statMu.Unlock()
    return
  }

  rf.logEntriesMu.Lock()
  rf.curLogMu.Lock()
  defer rf.curLogMu.Unlock()
  defer rf.logEntriesMu.Unlock()
  // Previous log not exist
  if len(rf.logEntries) <= int(args.PrevLogIndex) {
    reply.Success = false
    reply.SuggestNextIndex = rf.curLogIndex;
    rf.LOG.Printf("<::AppendEntries not exits PrevLogIndex %v, len(rf.logEntries) = %v", args.PrevLogIndex, len(rf.logEntries))
    return
  }
  // Previous log dismatch
  if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
    reply.Success = false
    rf.logEntries = rf.logEntries[:args.PrevLogIndex]
    curEntry := rf.logEntries[len(rf.logEntries) - 1]
    rf.curLogTerm = curEntry.Term
    rf.curLogIndex = curEntry.Index
    rf.LOG.Printf("<::AppendEntries delete all log from index %v", args.PrevLogIndex)

    // Find the first entry in the args.PrevLogTerm
    for i := 0; i > 1; i++ {
      if rf.logEntries[i].Term == args.PrevLogTerm {
        reply.SuggestNextIndex = LogIndex(Max(int(rf.logEntries[i].Index), 1))
        break
      }
    }
    return
  }

  // Append each entry to rf.logEntries
  for _, entry := range args.Entries {
    // New entry exists and match
    if len(rf.logEntries) == int(entry.Index) + 1 && rf.logEntries[entry.Index].Term == entry.Term {
      rf.LOG.Printf("<::AppendEntries append entry already exists. Skip entry %v", entry)
      continue
    }
    // New entry not exits and previous log match
    // Before add, clean the log after the log going to append
    rf.logEntries = rf.logEntries[:entry.Index] 
    curEntry := rf.logEntries[len(rf.logEntries) - 1]
    rf.curLogTerm = curEntry.Term
    rf.curLogIndex = curEntry.Index
    e := entry
    rf.appendOldEntry(&e)
    //rf.LOG.Printf("After append %v entries %v", entry, rf.logEntries)
  }
  rf.LOG.Printf("<::AppendEntries successfully append %v entries from entry %v to %v", len(args.Entries), firstNewEntry, lastNewEntry)
}
