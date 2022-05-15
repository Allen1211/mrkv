package raft

import (
	"github.com/Allen1211/mrkv/internal/netw"
	"github.com/Allen1211/mrkv/pkg/common"
)

//go:generate msgp
//msgp:ignore ApplyMsg

type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int
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
	Start		 int64
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
	Ts		int64
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

type TransferLeaderArgs struct {
	*netw.RPCArgBase

	Gid			int
	NodeId		int
	Peer		int
}

type TransferLeaderReply struct {
	Err common.Err
}

type TimeoutNowArgs struct {
	*netw.RPCArgBase
	Term 		int
}

type TimeoutNowReply struct {
	Term		int
	Success		bool
}