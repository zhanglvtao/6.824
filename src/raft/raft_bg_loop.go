package raft
import (
	"time"
)

func (rf *Raft) appendLoop() {
	for !rf.killed() {
		rf.statMu.Lock()
		curRole := rf.role
		rf.statMu.Unlock()
		if curRole != RaftRoleLeader {
			continue
		}
		rf.InLeader()
		time.Sleep(50 * time.Millisecond)
	}
}

// The tickerLoop go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// Single goroutine
func (rf *Raft) tickerLoop() {
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
		go rf.InCandidate()
	}
}