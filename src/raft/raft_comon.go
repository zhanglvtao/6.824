package raft
import (
  "fmt"
)

func (rf *Raft) SyncGetRaftStat() (Term, RaftRole, RaftId)  {
  rf.statMu.RLock()
  defer rf.statMu.RUnlock()
  return rf.curTerm, rf.role, rf.voteFor
}

func (rf *Raft) SyncGetCommitIndex() LogIndex {
  rf.commitIdxMu.RLock()
  defer rf.commitIdxMu.RUnlock()
  return rf.commitIndex
}

func(rf *Raft) SyncSetCommitIndex(idx LogIndex) LogIndex {
  rf.commitIdxMu.Lock()
  defer rf.commitIdxMu.Unlock()
  old := rf.commitIndex
  rf.commitIndex = idx
  return old
}

func(rf *Raft) SyncGetLogEntry(logIndex LogIndex) LogEntry {
  rf.logEntriesMu.RLock()
  defer rf.logEntriesMu.RUnlock()
  entry := *rf.logEntries[logIndex]
  if entry.Index != logIndex {
    rf.LOG.Fatalf("LogEntries idx(%v) dismatch LogEntry.Index(%v)", logIndex, entry.Index)
  }
  return entry
}

func(rf *Raft) SyncGetLogEntryLenCap() (int, int) {
  rf.logEntriesMu.RLock()
  defer rf.logEntriesMu.RUnlock()
  return len(rf.logEntries), cap(rf.logEntries)
}

func(rf *Raft) SyncSetMatchIndex(id RaftId, idx LogIndex) LogIndex {
  rf.matchIdxMu.Lock()
  defer rf.matchIdxMu.Unlock()
  oldLogIndex := rf.matchIndex[id]
  rf.matchIndex[id] = idx
  return oldLogIndex
}

func(rf *Raft) SyncGetMatchIndex(id RaftId) LogIndex {
  rf.matchIdxMu.RLock()
  defer rf.matchIdxMu.RUnlock()
  return rf.matchIndex[id]
}

func(rf *Raft) SyncGetNextIndex(id RaftId) LogIndex {
  rf.nextIdxMu.RLock()
  defer rf.nextIdxMu.RUnlock()
  return rf.nextIndex[id]
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
  rf.LOG.Printf("Init next index %v", curLogIndex + 1)
}

func (rf *Raft) AppendLogEntry(entry *LogEntry) {
  idx := entry.Index
  rf.logEntriesMu.Lock()
  defer rf.logEntriesMu.Unlock()
  if int(idx) != len(rf.logEntries) {
    rf.LOG.Fatalf("Log entres length %v dismatch log index %v", len(rf.logEntries), idx)
    return
  }
  if int(idx) == cap(rf.logEntries) {
    newLogEntries := make([]*LogEntry, cap(rf.logEntries) * 2)
     copy(newLogEntries, rf.logEntries)
     rf.logEntries = newLogEntries
     rf.LOG.Printf("Expand logEntries capacity to %v", cap(rf.logEntries))
  }
  rf.logEntries = append(rf.logEntries, entry)
  rf.LOG.Printf("Appended entry to leader : %v", entry)
}

func (rf *Raft) String() string {
  return fmt.Sprintf(
    "id %v, curLogIndex: %v, curLogTerm %v, commitIndex %v, lastApplied %v, curTerm %v, voteFor %v, role %v, lastTimeOut %v, lastTickTime %v",
    rf.id, rf.curLogIndex, rf.curLogTerm, rf.commitIndex, rf.lastApplied, rf.curTerm, rf.voteFor, rf.role, rf.lastTimeOut, rf.lastTickTime)
}