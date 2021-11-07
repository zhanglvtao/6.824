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
  //  "bytes"
  "fmt"
  "log"
  "math/rand"
  "os"
  "sync"
  "sync/atomic"
  "time"

  //  "6.824/labgob"
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

type Entry struct {
}

//
// A Go object implementing a single Raft peer.
//
type Term int64
type RaftId int64
type LogIndex uint64
type RaftRole string

const (
  RaftRoleLeader    RaftRole = "Leader"
  RaftRoleCandidate RaftRole = "Candidate"
  RaftRoleFollower  RaftRole = "Follower"
)

const	InvalidRaftId 		RaftId = -1
const InvalidRaftTerm 	Term = -1
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
  id             RaftId

  curLogIndex    LogIndex
  curLogTerm     Term
  commitIndex    LogIndex // index of the highest log entry known to be committed
  lastApplied    LogIndex // index of the highest log entry applied to state machine

  statusMu       *sync.Mutex
  curTerm        Term
  voteFor        RaftId
  role           RaftRole

  tickMu         *sync.Mutex
  lastTimeOut    time.Duration // Nanosecond
  lastTickTime   time.Duration // AppendEntries write <=> Election timer read

	LOG 					 *log.Logger

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
  // Your code here (2A).
  return int(rf.curTerm), rf.role == RaftRoleLeader
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

func (rf *Raft) tickTimeOut() time.Duration {
  rf.tickMu.Lock()
  defer rf.tickMu.Unlock()
  return rf.lastTickTime + rf.lastTimeOut - time.Duration(time.Now().UnixNano())
}

func (rf *Raft) refreshTimeOut() {
  rf.tickMu.Lock()
  defer rf.tickMu.Unlock()
  rf.lastTickTime = time.Duration(time.Now().UnixNano())
  rf.lastTimeOut = (time.Duration)(rand.Intn(100))%15*time.Millisecond*10 + time.Millisecond*150
}

func (rf *Raft) InCandidate() {
  rf.statusMu.Lock()
  defer rf.statusMu.Unlock()
  // Convert to condidate
  rf.role = RaftRoleCandidate
  // Increment curTerm
	old := rf.curTerm
  rf.curTerm = old + 1
	rf.LOG.Printf("Increment curTerm from %v to %v", old, rf.curTerm)
  // Vote for itself
  rf.voteFor = RaftId(rf.me)
  // Send RPC
  args := RequestVoteArgs{
    CandidateTerm:         rf.curTerm,
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
			VoterTerm: InvalidRaftTerm,
			VoterId: RaftId(peer),
			VoteGranted: false}
    count++
		rf.LOG.Printf("ChRequestVote to %v", peer)
    go rf.ChRequestVote(ch, peer, &args, &reply)
  }
	//rf.LOG.Println("DEBUG: waiting for RPC reply")
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
    if granted >= majority {
			rf.LOG.Println("Recevied majortiy peers' RPC::RequestVoteReply")
      break
    }
		if count == 0 {
			close(ch)
			rf.LOG.Printf("Received all peers' RPC::RequestVoteReply. Close channel")
			break
		}
  }

  if granted < majority {
    rf.LOG.Printf("Lose election, %v votes", granted)
    rf.role = RaftRoleFollower
    rf.curTerm = rf.curTerm - 1
    rf.voteFor = InvalidRaftId
    return
  }

  rf.LOG.Printf("Win election, %v votes", granted)
  //if (rf.role != RaftRoleCandidate) {
  //  rf.LOG.Printf("Not candidate! Stop converting to Leader")
  //  return
  //}
  rf.LOG.Printf("Convert from %v to Leader", rf.role)
  rf.role = RaftRoleLeader
  go rf.appendEntriesLoop()
}

// The tickerLoop go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// Single goroutine
func (rf *Raft) tickerLoop() {
  for rf.killed() == false {
    // Your code here to check if a leader election should
    // be started and to randomize sleeping time using
    if rf.role == RaftRoleLeader {
      rf.refreshTimeOut()
      rf.LOG.Printf("- Leader Term %v", rf.curTerm)
      time.Sleep(rf.lastTimeOut)
      continue
    }
    // Part 1. Check whether received append entries
    timeToTimeOut := rf.tickTimeOut()
    if timeToTimeOut > 0 {
      time.Sleep(timeToTimeOut)
      continue
    }

    rf.LOG.Println("+ New election")
    // Part 2. incremental curTerm & vote itself & reset election timer && send RequestVote RPC
    // Reset election timer
    rf.refreshTimeOut()
    rf.LOG.Printf("Reset election timer tick at(%v), time out(%v)", rf.lastTickTime, rf.lastTimeOut)
    rf.InCandidate()
  }
}

func (rf *Raft) appendEntriesLoop() {
  for !rf.killed() && rf.role == RaftRoleLeader{
    for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
      args := AppendEntriesArgs {
        LeaderTerm: rf.curTerm,
        LeaderId: rf.id,
        PreLogIndex: rf.curLogIndex,
        PreLogTerm: rf.curLogTerm,
        Entries: make([]Entry, 0),
        LeaderCommit: rf.commitIndex}
      reply := AppendEntriesReply{}
      go rf.sendAppendEntries(peer, &args, &reply)
    }
    time.Sleep(time.Millisecond * 75)
  }
}

func (rf *Raft) String() string {
	return fmt.Sprintf(
		"id %v, curLogIndex: %v, curLogTerm %v, commitIndex %v, lastApplied %v, curTerm %v, voteFor %v, role %v, lastTimeOut %v, lastTickTime %v",
		 rf.id, rf.curLogIndex, rf.curLogTerm, rf.commitIndex, rf.lastApplied, rf.curTerm, rf.voteFor, rf.role, rf.lastTimeOut, rf.lastTickTime)
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
  LOG = *log.New(os.Stdout, "Raft Maker ", log.Ltime|log.Lshortfile)
  rf := &Raft{
    mu:          sync.Mutex{},
    id:          RaftId(me),
    curLogIndex: 0,
    curLogTerm:  0,
    statusMu:    &sync.Mutex{},
    curTerm:     0,
    voteFor:     InvalidRaftId,
    role:        RaftRoleFollower,
    tickMu:      &sync.Mutex{},
		LOG: 				 log.New(os.Stdout, fmt.Sprintf("Raft-%v ", me), log.Ltime|log.Lshortfile)}
  rf.peers = peers
  rf.persister = persister
  rf.me = me
  rf.refreshTimeOut()

  // Your initialization code here (2A, 2B, 2C).

  // initialize from state persisted before a crash
  rf.readPersist(persister.ReadRaftState())

  LOG.Printf("Make Raft %v", rf)
  // start ticker goroutine to start elections
  go rf.tickerLoop()

  return rf
}
