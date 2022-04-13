package raft

//go:generate msgp

type InstallSnapshotMsg struct {
	Data             []byte
	LastIncludedIdx  int
	LastIncludedTerm int
	LastIncludedEndLSN  uint64
}

type LogEntry struct {
	Command []byte
	Term    int
	Index   int
}