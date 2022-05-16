package raft

import (
	"sync/atomic"
	"time"

	"github.com/allen1211/mrkv/internal/netw"
)

func (rf *Raft) HasLogAtCurrentTerm() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	for i := rf.log.length() - 1; i >= 0; i-- {
		if rf.log.posAt(i).Term == rf.currTerm {
			return true
		} else if rf.log.posAt(i).Term < rf.currTerm {
			return false
		}
	}
	return false
}

func (rf *Raft) ReadIndex() (bool, int) {

	if rf.getRole() == RoleLeader {
		return rf.readIndexLeader()
	} else if rf.getRole() == RoleFollower {
		return rf.readIndexFollower()
	} else {
		return false, 0
	}
}

func (rf *Raft) ReadIndexFromFollower(args *ReadIndexFromFollowerArgs, reply *ReadIndexFromFollowerReply) (err error) {
	rf.mu.RLock()
	if args.Term > rf.currTerm {
		rf.mu.RUnlock()

		rf.mu.Lock()
		rf.whenHearBiggerTerm(args.Term)
		rf.mu.Unlock()

		reply.Success = false
		reply.IsLeader = false
		return
	}
	rf.mu.RUnlock()

	if rf.getRole() == RoleLeader {
		reply.Success, reply.ReadIdx = rf.readIndexLeader()
		reply.IsLeader = true
	} else {
		reply.Success, reply.IsLeader = false, false
	}

	return
}

func (rf *Raft) readIndexLeader() (bool, int) {
	if !rf.HasLogAtCurrentTerm() {
		return false, 0
	}
	rf.mu.RLock()
	if rf.commitIdx <= rf.log.CpIdx() {
		rf.mu.RUnlock()
		return false, 0
	}
	if commitIdxLog := rf.log.idxAt(rf.commitIdx); commitIdxLog.Term < rf.currTerm {
		rf.mu.RUnlock()
		return false, 0
	}
	readIdx := rf.commitIdx
	if rf.currTerm == rf.lease.term && time.Now().UnixNano() < rf.lease.to {
		rf.logger.Debugf("leader lease read : %v", rf.lease)
		rf.mu.RUnlock()
		return true, readIdx
	}
	rf.mu.RUnlock()

	ch := make(chan bool, 1)
	var pass int32 = 1
	var denied int32 = 0
	for i := 0; i < rf.peers; i++ {
		if i == rf.me {
			continue
		}
		go func(j int) {
			var args AppendEntriesArgs
			var reply AppendEntriesReply

			rf.mu.RLock()
			args = rf.generateAppendEntriesArgs(j)
			rf.mu.RUnlock()

			if ok := rf.sendAppendEntries(j, &args, &reply); ok && reply.Success {
				if rf.eqMajority(int(atomic.AddInt32(&pass, 1))) {
					ch <- true
				}
			} else {
				if rf.eqMajority(int(atomic.AddInt32(&denied, 1))) {
					ch <- false
				}
			}
		}(i)
	}

	if agreed := <-ch; !agreed {
		return false, 0
	}

	return true, readIdx
}

func (rf *Raft) readIndexFollower() (bool, int) {
	rf.mu.RLock()
	currTerm := rf.currTerm
	rf.mu.RUnlock()

	readIdx := 0
	for i, j := rf.leader, 0; j < rf.peers; i, j = (i + 1) % rf.peers, j + 1 {
		if i == rf.me {
			continue
		}
		args := ReadIndexFromFollowerArgs{
			RPCArgBase: &netw.RPCArgBase{
				Peer: i,
			},
			Term: currTerm,
		}
		reply := ReadIndexFromFollowerReply{}

		ok := rf.rpcFunc(netw.ApiReadIndexFromFollower, &args, &reply, i)
		rf.mu.RLock()
		if rf.currTerm > currTerm {	// outdated
			rf.mu.RUnlock()
			return false, 0
		}
		rf.mu.RUnlock()

		if reply.IsLeader {
			rf.mu.Lock()
			rf.leader = i
			rf.mu.Unlock()
			if ok && reply.Success {
				readIdx = reply.ReadIdx
				rf.logger.Infof("Follower %d readIndex from leader %d: %d", rf.me, i, readIdx)
			}
			break
		}
	}
	if readIdx == 0 {
		return false, 0
	}

	return true, readIdx
}

func (rf *Raft) generateAppendEntriesArgs(j int) AppendEntriesArgs {
	var from, prevLogIdx, prevLogTerm int
	var entries []LogEntry
	from = rf.posOf(rf.nextIdx[j])
	if from <= 0 {
		prevLogIdx = rf.log.CpIdx()
		prevLogTerm = rf.log.CpTerm()
	} else {
		prevLogIdx = rf.log.posAt(from-1).Index
		prevLogTerm = rf.log.posAt(from-1).Term
	}
	if from < 0 || from >= rf.log.length() {
		entries = make([]LogEntry, 0)
	} else {
		entries = rf.log.slice(from, -1)
	}
	args := AppendEntriesArgs{
		RPCArgBase: &netw.RPCArgBase{},
		Term:         rf.currTerm,
		LeaderId:     rf.me,
		PrevLogIdx:   prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIdx,
		Entries:      entries,
	}
	return args
}