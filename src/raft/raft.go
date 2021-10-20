package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Term uint64
type RaftId uint64
type LogIndex uint64
type RaftRole string

const (
	RaftRoleLeader    RaftRole = "Leader"
	RaftRoleCandidate RaftRole = "Candidate"
	RaftRoleFollower  RaftRole = "Follower"
)

// Base election time out.
// From paper, 150ms - 300ms. Ref to 5.6 Timing and availability
const BaseElectionTimeOut time.Duration = time.Millisecond * 150

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A:
	id RaftId

	curLogIndex LogIndex
	curLogTerm  Term

	statusMu *sync.Mutex
	curTerm  Term
	voteFor  RaftId   // May data race
	role     RaftRole // AppendEntries write <=> Eleection timer write

	tickMu       *sync.Mutex
	lastTimeOut  time.Duration // Nanosecond
	lastTickTime time.Duration // AppendEntries write <=> Election timer read
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
	VoterId			RaftId
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Condition 1: Term dismatch OR log index dismatch
	reply.VoterTerm = rf.curTerm
	reply.VoterId = rf.id
	if args.CandidateTerm < rf.curTerm || args.CandidateLastLogIndex < rf.curLogIndex {
		reply.VoteGranted = false
		return
	}
	// Condition 2: Vote
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// Follower's loop
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// Part 1. Check whether received append entries
		rf.tickMu.Lock()
		if rf.lastTickTime+rf.lastTimeOut >= (time.Duration)(time.Now().UnixNano()) {
			LOG.Println("No new election")
			rf.tickMu.Unlock()
			break
		}
		rf.tickMu.Unlock()

		LOG.Println("New election +")
		// Part 2. incremental curTerm & vote itself & reset election timer && send RequestVote RPC
		// Increment curTerm
		rf.curTerm = rf.curTerm + 1
		// Vote for itself
		rf.voteFor = RaftId(rf.me)
		// Reset election timer
		rf.tickMu.Lock()
		rf.lastTickTime = time.Duration(time.Now().UnixNano())
		rf.lastTimeOut = (time.Duration)(rand.Intn(100)) % 15 * time.Millisecond * 10
		LOG.Printf("Reset election timer tick(%v), time out(%v)", rf.lastTickTime.String(), rf.lastTimeOut.String())
		// Send RPC
		args := RequestVoteArgs{
			CandidateTerm:         rf.curTerm,
			CandidateId:           rf.id,
			CandidateLastLogIndex: rf.curLogIndex,
			CandidateLastLogTerm:  rf.curLogTerm}
		ch := make(chan *RequestVoteReply)
		count := 0
		for peer := range rf.peers {
			reply := RequestVoteReply{}
			count++
			go rf.ChRequestVote(ch, peer, &args, &reply)
		}
		majority := count/2 + 1
		granted := 0
		for reply := range ch {
			count--
			if count == 0 {
				LOG.Printf("Lost election, %v votes", granted)
				break
			}
			if reply.VoteGranted {
				granted++
				LOG.Printf("< Raft-%v granted, term %v", reply.VoterId, reply.VoterTerm)
			} else {
				LOG.Printf("< Raft-%v rejected, term %v", reply.VoterId, reply.VoterTerm)
			}
			if granted == majority {
				close(ch)
				LOG.Println("Win election")
			}
		}
		// (TODO 10/20) If cur is NOT candidate, so it can't convert to Leader
		// Consider above Part2 and below as Atomic ?
		LOG.Printf("Raft-%v convert from %v to Leader", rf.id, rf.role)
		rf.role = RaftRoleLeader
	}
}

func (rf* Raft) ChRequestVote(ch chan *RequestVoteReply, server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.sendRequestVote(server, args, reply)
	ch <- reply
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
var LOG log.Logger

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	LOG = *log.New(os.Stdout, "Raft-"+strconv.Itoa(me), log.Ldate|log.Lshortfile)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
