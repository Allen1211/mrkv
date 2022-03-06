package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original mem_persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"io"
	"log"
	"os"
	"sync"
)

type MemoryPersister struct {
	mu        sync.Mutex
	raftstate []byte
	logstate  []byte
	snapshot  []byte
}

func MakeMemoryPersister() *MemoryPersister {
	return &MemoryPersister{}
}

func (ps *MemoryPersister) Copy() *MemoryPersister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakeMemoryPersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	np.logstate = ps.logstate
	return np
}

func (ps *MemoryPersister) DeepCopyLogFile(toFileName string) *MemoryPersister {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	np := MakeMemoryPersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	np.logstate = []byte(toFileName)

	fromFileName := string(ps.logstate)

	if fromFileName != "" && fromFileName != toFileName {
		from, err := os.Open(fromFileName)
		if err != nil {
			log.Fatalln(err)
		}
		to, err := os.OpenFile(toFileName, os.O_RDWR|os.O_TRUNC|os.O_CREATE, os.ModePerm)
		if err != nil {
			log.Fatalln(err)
		}
		if _, err := io.Copy(to, from); err != nil {
			log.Fatalln(err)
		}
		log.Printf("copy file from %s to %s\n", from, to)
	}
	return np


}

func (ps *MemoryPersister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
}

func (ps *MemoryPersister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

func (ps *MemoryPersister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *MemoryPersister) SaveLogState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.logstate = state
}

func (ps *MemoryPersister) ReadLogState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.logstate
}

func (ps *MemoryPersister) LogStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.logstate)
}

func (ps *MemoryPersister) SaveStateAndSnapshot(state []byte, logstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.logstate = logstate
	ps.snapshot = snapshot
}

func (ps *MemoryPersister) SaveSnapshot(snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.snapshot = snapshot
}

func (ps *MemoryPersister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *MemoryPersister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
