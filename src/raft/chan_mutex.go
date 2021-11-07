package raft

import "log"

type ChanMutex struct {
	ch chan struct{}
}

func NewChanMutex() *ChanMutex {
	ret := &ChanMutex{make(chan struct{}, 1)}
	ret.ch <- struct{}{}
	return ret
}

func(cm *ChanMutex) Lock() {
	<- cm.ch
}

func(cm *ChanMutex) Unlock() {
	select {
	case cm.ch <- struct{}{}:
	default:
		log.Fatalf("Unlock unlocked ChanMutex!")
	}
}

func(cm *ChanMutex) TryLock() bool {
	select {
	case <- cm.ch:
		return true
	default:
		return false
	}
}