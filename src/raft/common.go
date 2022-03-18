package raft

import (
	"mrkv/src/netw"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type InstallSnapshotMsg struct {
	Data             []byte
	LastIncludedIdx  int
	LastIncludedTerm int
	LastIncludedEndLSN  uint64
}

type EmptyCmd struct {
}

type RequestVoteArgs struct {
	*netw.RPCArgBase

	Term        int
	CandidateId int
	LastLogIdx  int
	LastLogTerm int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}


type AppendEntriesArgs struct {
	*netw.RPCArgBase

	Term         int
	LeaderId     int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}


type InstallSnapshotArgs struct {
	*netw.RPCArgBase
	Term             int
	LeaderId         int
	LastIncludedIdx  int
	LastIncludedTerm int
	LastIncludedEndLSN	uint64
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}


type ReadIndexFromFollowerArgs struct {
	*netw.RPCArgBase
	Term  int
}

type ReadIndexFromFollowerReply struct {
	Term 		int
	IsLeader 	bool
	Success		bool
	ReadIdx		int
}