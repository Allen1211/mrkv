package raft

type Persister interface {
	Copy() Persister
	SaveRaftState(state []byte)
	ReadRaftState() []byte
	RaftStateSize() int
	SaveStateAndSnapshot(state []byte, snapshot []byte)
	ReadSnapshot() []byte
	SnapshotSize() int

}
