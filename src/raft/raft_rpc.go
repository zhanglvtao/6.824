package raft

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
  FollowTerm  Term
  Success     bool
}

func (rf *Raft) ChRequestVote(ch chan *RequestVoteReply, server int, args *RequestVoteArgs, reply *RequestVoteReply) {
  ok := rf.sendRequestVote(server, args, reply)
	if !ok {
	  rf.LOG.Printf("- RPC::RequestVote(SendTo %v) Fail", reply.VoterId)
	}
	rf.LOG.Printf("DEBUG: Received reply ChRequestVote")
  ch <- reply
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  // Your code here (2A, 2B).
  // Condition 1: Term dismatch OR log index dismatch
  reply.VoterTerm = rf.curTerm
  reply.VoterId = rf.id
	if args.CandidateTerm <= rf.curTerm {
		reply.VoteGranted = false
		rf.LOG.Printf("> Reject vote candidate %v, term %v left behind %v", args.CandidateId, args.CandidateTerm, rf.curTerm)
		return
	}
  if args.CandidateLastLogIndex < rf.curLogIndex {
    reply.VoteGranted = false
		rf.LOG.Printf("> Reject vote candidate %v, log-index %v left behind of %v", args.CandidateId, args.CandidateLastLogIndex, rf.curLogIndex)
    return
  }
	//if rf.voteFor != InvalidRaftId {
	//	rf.LOG.Printf("> Reject vote, has granted to %v", rf.voteFor)
	//}
  // Condition 2: Vote
	rf.statusMu.Lock()
	if args.CandidateTerm <= rf.curTerm {
		reply.VoteGranted = false
		rf.LOG.Printf("> Reject vote candidate %v, term %v left behind %v", args.CandidateId, args.CandidateTerm, rf.curTerm)		
		return
	}
	if args.CandidateLastLogIndex < rf.curLogIndex {
    reply.VoteGranted = false
		rf.LOG.Printf("> Reject vote candidate %v, log-index %v left behind of %v", args.CandidateId, args.CandidateLastLogIndex, rf.curLogIndex)
    return
  }
	defer rf.statusMu.Unlock()
  rf.voteFor = args.CandidateId
	old := rf.curTerm
	rf.curTerm = args.CandidateTerm
	rf.role = RaftRoleFollower
  reply.VoteGranted = true
	rf.LOG.Printf("> Vote for %v, term %v => %v", rf.voteFor, old, rf.curTerm)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  return ok
}

// As receiver
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
  if args.LeaderTerm < rf.curTerm {
    reply.Success = false
		reply.FollowTerm = rf.curTerm
    rf.LOG.Printf("+ RPC::AppendEntries Raft-%v should convert to follower", args.LeaderId)//. Term dismatch follower %v, leader %v", rf.curTerm, args.LeaderTerm)
    return
  }
  reply.Success = true
	reply.FollowTerm = rf.curLogTerm

	rf.statusMu.Lock()
  if args.LeaderTerm < rf.curTerm {
    reply.Success = false
		reply.FollowTerm = rf.curTerm
    rf.LOG.Printf("+ RPC::AppendEntries Raft-%v should convert to follower", args.LeaderId)//. Term dismatch follower %v, leader %v", rf.curTerm, args.LeaderTerm)
    return
  }
	rf.role = RaftRoleFollower
	rf.statusMu.Unlock()
	rf.refreshTimeOut()
	rf.LOG.Printf("+ RPC::AppendEntries")// Refresh lastTickTime %v, lastTimeOut %v", rf.lastTickTime, rf.lastTimeOut)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
  ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if reply.FollowTerm > rf.curLogTerm {
		rf.statusMu.Lock()
		defer rf.statusMu.Unlock()
		rf.role = RaftRoleFollower
		rf.LOG.Printf("sendAppendEntries: Leader => %v", rf.role)
	}
  return ok
}

