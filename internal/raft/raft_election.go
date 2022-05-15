package raft

import (
	"sync"
	"sync/atomic"

	"github.com/Allen1211/mrkv/internal/netw"
	"github.com/Allen1211/mrkv/pkg/common"
	"github.com/Allen1211/mrkv/pkg/common/utils"
)

const leaseNano = int64(1500000000)
// const leaseNano = int64(100000000)

type lease struct {
	term 	int
	start   int64
	to		int64
}

func (l *lease) refresh(term int, to int64)  {
	if term >= l.term {
		l.term = term
	}
	if to > l.to	 {
		l.to   = to
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) (err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.electionTimer.Reset(rf.generateElectionTimeout())

	if args.Term > rf.currTerm {
		rf.whenHearBiggerTerm(args.Term)
	}
	originVoteFor := rf.voteFor

	reply.VoteGranted = func() bool {
		if args.Term < rf.currTerm || (args.Term == rf.currTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
			return false
		}
		if rf.log.length() == 0 {
			return true
		}
		lastLog := rf.log.last()
		rf.logger.Debugf("Follower %d last log: %v Candidate lastLogTerm: %d lastLogIdx: %d", rf.me, lastLog, args.LastLogTerm, args.LastLogIdx)

		return lastLog.Term < args.LastLogTerm || (lastLog.Term == args.LastLogTerm && lastLog.Index <= args.LastLogIdx)
	}()

	rf.logger.Infof("Peer %d hear RequestVote from candidate %d, candidate term is %d, its term is %d, voteFor is %d vote granted: %v", rf.me, args.CandidateId, args.Term, rf.currTerm, originVoteFor, reply.VoteGranted)

	if reply.VoteGranted {
		rf.voteFor = args.CandidateId
		rf.persist()
	}

	reply.Term = rf.currTerm
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.rpcFunc(netw.ApiRequestVote, args, reply, server)
}

func (rf *Raft) TransferLeader(args *TransferLeaderArgs, reply *TransferLeaderReply) (err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	transferee := args.Peer

	rf.logger.Infof("Leadership transfer begin: target is %d", transferee)

	if transferee < 0 || transferee >= rf.peers {
		rf.logger.Errorf("invalid transferee %d, peers %d", transferee, rf.peers)

		reply.Err = common.ErrFailed
		return
	}
	if rf.getRole() != RoleLeader {
		rf.logger.Errorf("can't transfer: not leader")
		reply.Err = common.ErrWrongLeader
		return
	}
	if transferee == rf.me {
		rf.logger.Errorf("transferee is self")
		if rf.getRole() == RoleLeader {
			reply.Err = common.OK
			return
		} else {
			reply.Err = common.ErrFailed
			return
		}
	}

	if rf.leaderTransferring {	// currently in progress
		rf.logger.Infof("last leadership transferee eq to transferee %d, transfering in progress", transferee)
		reply.Err = common.ErrFailed
		return
	}

	rf.leaderTransferring = true
	rf.electionTimer.Reset(rf.generateElectionTimeout())
	lastIdx, lastTerm := rf.log.lastIdxTerm()
	rf.logger.Infof("leadership transfer: lastIdx=%d, lastTerm=%d, currTerm: %d", lastIdx, lastTerm, rf.currTerm)

	for rf.matchIdx[transferee] != lastIdx {

		rf.logger.Infof("leadership transfer: lastIdx=%d, matchIdx=%d, need to catch up", lastIdx, rf.matchIdx[transferee])

		if !rf.leaderTransferring {
			rf.logger.Errorf("can't transfer: aborted")
			reply.Err = common.ErrFailed
			return
		}
		if rf.getRole() != RoleLeader {
			rf.logger.Errorf("can't transfer: not leader")
			reply.Err = common.ErrFailed
			rf.leaderTransferring = false
			return
		}

		rf.mu.Unlock()
		rf.replicateOneRound(transferee, 0)
		rf.mu.Lock()
	}

	rf.logger.Infof("leadership transfer: transfee catch up to %d, ready send timeout", lastIdx)

	timeoutArgs := TimeoutNowArgs{
		RPCArgBase: &netw.RPCArgBase{
			Peer: transferee,
		},
		Term: rf.currTerm,
	}
	timeoutReply := TimeoutNowReply{}

	if ok := rf.sendTimeout(transferee, &timeoutArgs, &timeoutReply); !ok || !timeoutReply.Success {
		rf.logger.Errorf("leadership transfer: failed to send timeout, reply: %+v", timeoutReply)
		rf.leaderTransferring = false
		reply.Err = common.ErrFailed
		return
	}

	rf.logger.Infof("leadership transfer: finished send timeout")

	reply.Err = common.OK
	rf.leaderTransferring = false
	return
}

func (rf *Raft) sendTimeout(peer int, args *TimeoutNowArgs, reply *TimeoutNowReply) bool {
	return rf.rpcFunc(netw.ApiTimeoutNow, args, reply, peer)
}

func (rf *Raft) TimeoutNow(args *TimeoutNowArgs, reply *TimeoutNowReply) (err error) {

	rf.mu.RLock()

	if args.Term != rf.currTerm {
		rf.logger.Errorf("failed to timeout now, term not match: argTerm=%d, currTerm=%d", args.Term, rf.currTerm)
		reply.Term = rf.currTerm
		reply.Success = false
		rf.mu.RUnlock()
		return
	}

	rf.electionCh <- rf.currTerm

	reply.Success = true
	reply.Term = rf.currTerm

	rf.mu.RUnlock()
	return
}

func (rf *Raft) whenHearBiggerTerm(term int) {
	rf.logger.Infof("Peer %d hear from peer with bigger term %d > %d, update current term",
		rf.me, term, rf.currTerm)
	rf.currTerm = term
	rf.voteFor = -1
	rf.persist()
	if rf.getRole() != RoleFollower {
		isLeaderBefore := rf.getRole() == RoleLeader
		rf.setRole(RoleFollower)
		rf.logger.Infof("Peer %d is not follower while hear bigger term, switch to follower", rf.me)
		if isLeaderBefore {
			rf.logger.Infof("Peer %d is old leader switch to follower and start electionLoop", rf.me)
		}
	}
}

// election process
func (rf *Raft) doElection() {
	rf.mu.Lock()
	if rf.getRole() == RoleLeader {
		rf.mu.Unlock()
		return
	}

	// swich to cadidate
	rf.setRole(RoleCandidate)
	// incr term
	rf.currTerm++
	// vote for self
	rf.voteFor = rf.me
	rf.persist()

	electionTerm := rf.currTerm
	rf.mu.Unlock()

	// send request vote rpc to all other server
	var passVote int32 = 1
	var once sync.Once
	for i := 0; i < rf.peers; i++ {
		if i == rf.me {
			continue
		}
		go func(j int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			var lastLogTerm, lastLogIdx int = 0, 0
			if rf.log.length() > 0 {
				lastLogTerm = rf.log.last().Term
				lastLogIdx = rf.log.last().Index
			}
			args := RequestVoteArgs{
				RPCArgBase: &netw.RPCArgBase{},
				Term:        electionTerm,
				CandidateId: rf.me,
				LastLogIdx:  lastLogIdx,
				LastLogTerm: lastLogTerm,
			}
			reply := RequestVoteReply{}

			rf.mu.Unlock()
			if ok := rf.sendRequestVote(j, &args, &reply); !ok {
				rf.logger.Debugf("Candidate %d Fail to send RequestVote rpc to peer %d", rf.me, j)
				rf.mu.Lock()
				return
			}
			rf.mu.Lock()

			if rf.currTerm != args.Term {
				return
			}

			if reply.Term > rf.currTerm {
				rf.whenHearBiggerTerm(reply.Term)
				return
			}

			//rf.logger.Debugf("Candidate %d send RequestVote to peer %d reply: %v", rf.me, j, reply)
			if reply.VoteGranted {
				if rf.getRole() == RoleFollower {
					// has already heard from new leader
					rf.logger.Infof("Candidate %d already heard heartbeat from new leader, stop election", rf.me)
					return
				}

				voteCnt := atomic.AddInt32(&passVote, 1)
				rf.logger.Infof("Candidate %d receive a vote from peer %d", rf.me, j)
				if int(voteCnt) >= rf.peers/2+1 {
					once.Do(func() {
						rf.logger.Infof("Candidate %d %v win the election(%d/%d) become leader", rf.me, rf, voteCnt, rf.peers)
						rf.setRole(RoleLeader)
						rf.reInitNextIdx()
						rf.matchIdx = make([]int, rf.peers)

						go rf.Start(utils.EncodeCmdWrap(common.CmdTypeEmpty, []byte{}))
					})
				}
			}
		}(i)
	}
}