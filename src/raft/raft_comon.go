package raft

import (
	"fmt"
)

func (rf *Raft) SyncGetRaftStat() (Term, RaftRole, RaftId) {
	rf.statMu.RLock()
	defer rf.statMu.RUnlock()
	return rf.curTerm, rf.role, rf.voteFor
}

func (rf *Raft) SyncGetCommitIndex() LogIndex {
	rf.commitIdxMu.RLock()
	defer rf.commitIdxMu.RUnlock()
	return rf.commitIndex
}

func (rf *Raft) SyncSetCommitIndex(idx LogIndex) LogIndex {
	rf.commitIdxMu.Lock()
	defer rf.commitIdxMu.Unlock()
	old := rf.commitIndex
	rf.commitIndex = idx
	return old
}

func (rf *Raft) SyncGetLogEntry(logIndex LogIndex) LogEntry {
	rf.logEntriesMu.RLock()
	defer rf.logEntriesMu.RUnlock()
	if int(logIndex) >= len(rf.logEntries) {
		rf.LOG.Fatalf("Try access invalid entry with index %v, len %v", logIndex, len(rf.logEntries))
	}
	entry := *rf.logEntries[logIndex]
	if entry.Index != logIndex {
		rf.LOG.Fatalf("Conflict rf.logEntries[%v].index = %v. rf.logEntries[] %v", logIndex, entry.Index, rf.logEntries)
	}
	return entry
}

func (rf *Raft) SyncGetLogEntryLenCap() (int, int) {
	rf.logEntriesMu.RLock()
	defer rf.logEntriesMu.RUnlock()
	return len(rf.logEntries), cap(rf.logEntries)
}

func (rf *Raft) SyncSetMatchIndex(id RaftId, idx LogIndex) LogIndex {
	rf.matchIdxMu.Lock()
	defer rf.matchIdxMu.Unlock()
	oldLogIndex := rf.matchIndex[id]
	rf.matchIndex[id] = idx
	return oldLogIndex
}

func (rf *Raft) SyncGetMatchIndex(id RaftId) LogIndex {
	rf.matchIdxMu.RLock()
	defer rf.matchIdxMu.RUnlock()
	return rf.matchIndex[id]
}

func (rf *Raft) SyncGetNextIndex(id RaftId) LogIndex {
	rf.nextIdxMu.RLock()
	defer rf.nextIdxMu.RUnlock()
	return rf.nextIndex[id]
}

func(rf *Raft) SyncGetCurLogIndexTerm() (LogIndex, Term) {
	rf.curLogMu.RLock()
	defer rf.curLogMu.RUnlock()
	return rf.curLogIndex, rf.curLogTerm
}

func (rf *Raft) InitNextIndex() {
	rf.nextIdxMu.Lock()
	defer rf.nextIdxMu.Unlock()
	rf.curLogMu.Lock()
	curLogIndex := rf.curLogIndex
	rf.curLogMu.Unlock()
	for i := range rf.nextIndex {
		rf.nextIndex[i] = curLogIndex + 1
	}
	rf.LOG.Printf("Init next index %v", curLogIndex+1)
}

func (rf *Raft) InitLogEntries() {
	entry := &LogEntry{ Term: RaftTermInitial, Index: LogIndexInitial, Command: CommandInitial }
	rf.logEntries = append(rf.logEntries, entry)
	rf.curLogIndex = LogIndexInitial
	rf.curLogTerm = RaftTermInitial
	rf.LOG.Printf("Init log entries. Len %v, cap %v, curLogIndex %v, curLogTerm %v", len(rf.logEntries), cap(rf.logEntries), rf.curLogIndex, rf.curLogTerm)
}

// Note: Assume the entry is legal to append. 
//       Require rf.curLogMu first.
//       Require rf.logEntriesMu first.
func (rf *Raft) appendOldEntry(entry *LogEntry) {
  rf.appendEntry(entry)
  rf.curLogIndex = entry.Index
  rf.curLogTerm = entry.Term
  rf.LOG.Printf("Appended old log entry %v", *entry)
}

func (rf *Raft) appendNewEntry(command interface{}) LogEntry {
	newEntryTerm, _, _ := rf.SyncGetRaftStat()
	rf.curLogMu.Lock()
  rf.logEntriesMu.Lock()
	newEntryIndex := rf.curLogIndex + 1
	newEntry := LogEntry{Index: newEntryIndex, Term: newEntryTerm, Command: command}
	rf.appendEntry(&newEntry)

	rf.curLogIndex = newEntryIndex
  rf.curLogTerm = newEntryTerm

  rf.logEntriesMu.Unlock()
	rf.curLogMu.Unlock()
	rf.LOG.Printf("Append new log entry %v", newEntry)
  return newEntry
}

// Note: Require rf.logEntriesMu write first, make sure entry.Index == len(rf.logEntries)
func (rf *Raft) appendEntry(entry *LogEntry) {
	l := len(rf.logEntries)
	c := cap(rf.logEntries)
  if l != int(entry.Index) {
    rf.LOG.Fatalf("Conflict entry.Index(%v) /= len(rf.logEntries)(%v)", entry.Index, l)
  }
	if int(entry.Index) == c {
		newLogEntries := make([]*LogEntry, l*2)
		copy(newLogEntries, rf.logEntries)
		rf.logEntries = newLogEntries
		rf.LOG.Printf("Expand logEntries capacity to %v", c)
	}
	rf.logEntries = append(rf.logEntries, entry)
}

func (rf *Raft) String() string {
	return fmt.Sprintf(
		"id %v, curLogIndex: %v, curLogTerm %v, commitIndex %v, lastApplied %v, curTerm %v, voteFor %v, role %v, lastTimeOut %v, lastTickTime %v",
		rf.id, rf.curLogIndex, rf.curLogTerm, rf.commitIndex, rf.lastApplied, rf.curTerm, rf.voteFor, rf.role, rf.lastTimeOut, rf.lastTickTime)
}

func Min(a int, b int) int {
  if a > b {
    return b
  }
  return a
}

func Max(a int, b int) int {
  if a > b {
    return a
  }
  return b
}