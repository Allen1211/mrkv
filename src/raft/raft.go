package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"mrkv/src/common"
	"mrkv/src/common/labgob"
	"mrkv/src/netw"
)


const (
	RoleFollower  int32 = 0
	RoleCandidate int32 = 1
	RoleLeader    int32 = 2
)

type Raft struct {
	mu        sync.RWMutex      // Lock to protect shared access to this peer's state
	peers     int // RPC end points of all peers
	persister *DiskPersister  // Object to hold this peer's persisted state
	me        int               // this peer's index into peers[]
	dead      int32             // set by Kill()

	leader	 int
	// Persistent state on all servers
	currTerm int
	voteFor  int

	log		raftlog
	logFileEnabled bool

	// Volatile state on all servers
	commitIdx   int
	lastApplied int

	// Volatile state on leaders
	nextIdx  []int
	matchIdx []int

	// 0->follower 1->candidate 2->leader
	role int32

	applyCh          chan ApplyMsg
	applyCond		 *sync.Cond

	checkpointCh	chan int

	replicatorCond  []*sync.Cond

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	rpcFunc 	netw.RpcFunc

	logger		*logrus.Logger
}

func Make(rpcFunc netw.RpcFunc, peers int, me int, persister *DiskPersister, applyCh chan ApplyMsg, logFileEnabled bool, logFileName string,
	logFileCap uint64, logLevel string) *Raft {

	rf := &Raft{}
	
	rf.logger, _ = common.InitLogger(logLevel, "Raft")

	rf.peers = peers
	rf.rpcFunc = rpcFunc
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.setRole(RoleFollower)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.logFileEnabled = logFileEnabled
	if logFileEnabled {
		rf.log = makeWriteAheadRaftLog(0, 0, rf, logFileName, logFileCap)
		rf.log.restore([]byte(logFileName))
	} else {
		rf.log = makeInMemoryRaftLog(0, 0, rf)
		rf.log.restore(persister.ReadLogState())
	}

	rf.commitIdx = rf.log.CpIdx()
	rf.lastApplied = rf.log.CpIdx()

	rf.nextIdx = make([]int, rf.peers)
	rf.matchIdx = make([]int, rf.peers)
	rf.reInitNextIdx()

	rand.Seed(time.Now().UnixNano())
	rf.electionTimer = time.NewTimer(rf.generateElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(rf.generateHeartbeatTimeout())

	go rf.ticker()
	go rf.applyer()

	rf.replicatorCond = make([]*sync.Cond, rf.peers)
	for i := 0; i < rf.peers; i++ {
		if i == rf.me {
			continue
		}
		rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
		go rf.replicator(i)
	}

	rf.checkpointCh = make(chan int, 10)
	go func() {
		for !rf.killed() {
			rf.mu.RLock()
			if rf.commitIdx > rf.lastApplied {
				rf.applyCond.Signal()
			}
			rf.mu.RUnlock()
			time.Sleep(100*time.Millisecond)
		}
	}()

	rf.logger.Infof("Raft peer %d Created", rf.me)

	return rf
}

func (rf *Raft) Clear() {
	rf.log.clear()
	rf.persister.Clear()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			if rf.killed() {
				return
			}

			rf.doElection()

			rf.mu.Lock()
			rf.electionTimer.Reset(rf.generateElectionTimeout())
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			if rf.killed() {
				return
			}

			// rf.sendHeartbeat()
			rf.BroadcastHeartbeat(true)

			rf.mu.Lock()
			rf.heartbeatTimer.Reset(rf.generateHeartbeatTimeout())
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) generateElectionTimeout() time.Duration {
	return time.Duration(1000 + rand.Intn(500)) * time.Millisecond
}

func (rf *Raft) generateHeartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func (rf *Raft) getRole() int32 {
	return atomic.LoadInt32(&rf.role)
}

func (rf *Raft) setRole(r int32) {
	atomic.StoreInt32(&rf.role, r)
}

func (rf *Raft) CheckpointCh() *chan int {
	return &rf.checkpointCh
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currTerm, rf.getRole() == RoleLeader
}

//
func (rf *Raft) persist() {
	data := rf.encodeRaftState()
	logData := rf.log.dump()
	data = append(data, logData...)
	rf.persister.SaveRaftState(data)
	rf.persister.SaveLogState(logData)

}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.voteFor)
	data := w.Bytes()
	return data
}

func (rf *Raft) encodeRaftStateWithLog() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.voteFor)
	data := w.Bytes()
	logData := rf.log.dump()
	data = append(data, logData...)
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm, voteFor int
	if d.Decode(&currTerm) != nil || d.Decode(&voteFor) != nil {
		log.Fatalf("Peer %d failed to readPersist", rf.me)
	} else {
		rf.currTerm = currTerm
		rf.voteFor = voteFor
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) (err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currTerm {
		rf.logger.Debugf("Follower %d has bigger term than Leader (%d > %d) in AE rpc, disagree", rf.me, rf.currTerm, args.Term)
		reply.Success = false
		reply.Term = rf.currTerm
		return
	}

	rf.electionTimer.Reset(rf.generateElectionTimeout())

	if args.Term > rf.currTerm {
		rf.whenHearBiggerTerm(args.Term)
	}

	doAgreement := func() bool {
		if rf.posOf(args.PrevLogIdx)+1 > rf.log.length() {
			rf.logger.Debugf("Follower %d don't have any log entry at index %d, disagree", rf.me, args.PrevLogIdx)
			// FastBackup
			reply.XTerm = -1
			reply.XIndex, _ = rf.log.lastIdxTerm()
			return false
		}
		if rf.posOf(args.PrevLogIdx) >= 0 && args.PrevLogTerm != rf.log.idxAt(args.PrevLogIdx).Term {
			rf.logger.Debugf("Follower %d prev log (%d) not match, need to truncate, disagree", rf.me, args.PrevLogIdx)
			// FaseBackup: conflicting log's term and the first log idx of the xterm
			reply.XTerm = rf.log.idxAt(args.PrevLogIdx).Term
			reply.XIndex = args.PrevLogIdx
			for rf.posOf(reply.XIndex-1) >= 0 {
				if rf.log.idxAt(reply.XIndex-1).Term != reply.XTerm {
					break
				}
				reply.XIndex--
			}
			return false
		}

		return true
	}
	reply.Success = doAgreement()

	if reply.Success {
		// agree, append log and update commitIdx
		if len(args.Entries) > 0 {
			rf.logger.Debugf("Follower %d agree with Leader at %d, need to append %d entries...",
				rf.me, args.PrevLogIdx, len(args.Entries))
		}
		for _, e := range args.Entries {
			rf.log.truncateAppend(e)
			if e.Index > rf.commitIdx && e.Index <= args.LeaderCommit && rf.log.length() > 0 && reply.Success {
				rf.commitIdx = e.Index
				rf.log.commitTo(rf.commitIdx)
				rf.logger.Infof("Follower %d: advanced commitIdx to %d", rf.me, rf.commitIdx)
				rf.applyCond.Signal()
			}
		}
		rf.log.sync()
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIdx && rf.log.length() > 0 && reply.Success {
		// some log entries after commitIdx were commited by leader, so follower should commit them either
		// logs between old commitId+1 and new commitIdx should be send to applyCh, then update lastApplied
		lastIdx := rf.log.last().Index
		if lastIdx > args.PrevLogIdx+len(args.Entries) {
			lastIdx = args.PrevLogIdx + len(args.Entries)
		}
		rf.commitIdx = min(args.LeaderCommit, lastIdx)
		rf.log.commitTo(rf.commitIdx)
		rf.logger.Infof("Follower %d: advanced commitIdx to %d", rf.me, rf.commitIdx)
		rf.applyCond.Signal()
	}

	reply.Term = rf.currTerm
	return
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	args.Peer = server
	return rf.rpcFunc(netw.ApiAppendEntries, args, reply, server)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply)(err error)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Infof("Follower %d receive InstallSnapshot RPC: currTerm=%d args.Term=%d, args.LastIncludedIdx=%d, args.LastIncludedTerm=%d", rf.me, rf.currTerm, args.Term, args.LastIncludedIdx, args.LastIncludedTerm)

	if rf.currTerm > args.Term {
		reply.Term = rf.currTerm
		return
	}
	if rf.currTerm < args.Term {
		rf.whenHearBiggerTerm(args.Term)
	}

	if args.LastIncludedIdx <= rf.commitIdx {
		rf.logger.Infof("Follower %d InstallSnapshot RPC: args.LastIncludedIdx(%d) <= rf.commitIdx(%d) ignoged",
			rf.me, args.LastIncludedIdx, rf.commitIdx)
		return
	}

	go func() {
		msg := ApplyMsg {
			CommandValid: true,
			Command: InstallSnapshotMsg {
				Data:             args.Data,
				LastIncludedTerm: args.LastIncludedTerm,
				LastIncludedIdx:  args.LastIncludedIdx,
				LastIncludedEndLSN: args.LastIncludedEndLSN,
			},
			CommandTerm: rf.currTerm,
		}
		rf.logger.Debugf("Peer %d ready to apply snapshot msg to application", rf.me)
		rf.applyCh <- msg
		rf.logger.Debugf("Peer %d applied snapshot msg to application", rf.me)
	}()


	reply.Term = rf.currTerm

	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.rpcFunc(netw.ApiInstallSnapshot, args, reply, server)

}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, lastIncludedLSN uint64, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Infof("Peer %d calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v",
		rf.me, lastIncludedTerm, lastIncludedIndex, rf.currTerm)

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIdx {
		rf.logger.Infof("Peer %d rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger",
			rf.me, lastIncludedIndex, rf.commitIdx)
		return false
	}
	if lastIncludedIndex <= rf.log.CpIdx() {
		rf.logger.Infof("Peer %d rejects the snapshot which lastIncludedIndex is %v because cpIdx %v is larger",
			rf.me, lastIncludedIndex, rf.log.CpIdx())
		return false
	}

	rf.log.compactTo(lastIncludedIndex, lastIncludedTerm, lastIncludedLSN)

	rf.lastApplied, rf.commitIdx = lastIncludedIndex, lastIncludedIndex
	rf.log.commitTo(rf.commitIdx)
	rf.log.applyTo(rf.lastApplied)

	rf.persister.SaveStateAndSnapshot(rf.encodeRaftStateWithLog(), snapshot)

	rf.logger.Infof("Peer %d accept CondInstallSnapshot now cpIdx=lastApplied=commitIdx=%d cpTerm=%d",rf.me, lastIncludedIndex, lastIncludedTerm)

	return true
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.getRole() != RoleLeader {
		return 0, 0, false
	}

	// append log entry to local logs
	rf.mu.Lock()
	var lastLogIdx int
	if rf.log.length() > 0 {
		lastLogIdx = rf.log.last().Index
	} else {
		lastLogIdx = rf.log.CpIdx()
	}
	entry := LogEntry {
		Command: command,
		Term:    rf.currTerm,
		Index:   lastLogIdx + 1,
	}
	rf.log.append(entry)
	rf.logger.Debugf("Leader %d append entry %v to logs", rf.me, entry.Index)
	rf.persist()
	rf.log.sync()
	rf.mu.Unlock()

	rf.BroadcastHeartbeat(false)
	return entry.Index, entry.Term, true
}


func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := 0; peer <  rf.peers; peer++ {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer)
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if rf.getRole() != RoleLeader {
		return false
	}
	lastIdx, _ := rf.getLastLogIdxTerm()
	return rf.matchIdx[peer] < lastIdx
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		// if there is no need to replicate entries for this peer, just release CPU and wait other goroutine's signal if service adds new Command
		// if this peer needs replicating entries, this goroutine will call replicateOneRound(peer) multiple times until this peer catches up, and then wait
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
			if rf.killed() {
				return
			}
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.getRole() != RoleLeader {
		rf.mu.RUnlock()
		return
	}

	prevLogIndex := rf.nextIdx[peer] - 1
	if prevLogIndex < rf.log.CpIdx() {
		// only snapshot can catch up
		rf.logger.Infof("Leader %d fastBackup follower %d need to send snapshot to help catch up", rf.me, peer)
		snapshot := rf.persister.ReadSnapshot()
		rf.mu.RUnlock()

		rf.doSendSnapshotToFollower(peer, rf.log.CpIdx(), rf.log.CpTerm(), snapshot)

	} else {
		// just entries can catch up

		var args AppendEntriesArgs
		var reply AppendEntriesReply

		var from, prevLogIdx, prevLogTerm int
		var entries []LogEntry
		from = rf.posOf(rf.nextIdx[peer])
		if from == 0 {
			prevLogIdx = rf.log.CpIdx()
			prevLogTerm = rf.log.CpTerm()
		} else {
			prevLogIdx = rf.log.posAt(from - 1).Index
			prevLogTerm = rf.log.posAt(from - 1).Term
		}
		if from >= rf.log.length() {
			entries = make([]LogEntry, 0)
		} else {
			entries = rf.log.slice(from, -1)
		}
		args = AppendEntriesArgs {
			RPCArgBase: &netw.RPCArgBase{},
			Term:         rf.currTerm,
			LeaderId:     rf.me,
			PrevLogIdx:   prevLogIdx,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIdx,
			Entries:      entries,
		}
		rf.mu.RUnlock()

		if rf.sendAppendEntries(peer, &args, &reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, &args, &reply)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) handleAppendEntriesResponse(j int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// out of date reply
	if rf.currTerm != args.Term {
		return
	}
	if rf.getRole() != RoleLeader {
		return
	}
	//rf.logger.Debugf("Leader %d send AE to follower %d,args: %v, reply: %v", rf.me, j, args, reply)

	// if disagree
	if !reply.Success {
		rf.logger.Debugf("Follower %d disagree with entries (Leader sent Term:%d, PrevLogIdx:%d,PrevLogTerm:%d), its term is %d xindx is %d xterm is %d",
			j, args.Term, args.PrevLogIdx, args.PrevLogTerm, reply.Term, reply.XIndex, reply.XTerm)

		if rf.currTerm < reply.Term {
			rf.whenHearBiggerTerm(reply.Term)
			return
		}

		// desc nextIdx and keep retrying utils matches
		if rf.posOf(rf.nextIdx[j]) == 0 {
			// return
		}
		if rf.posOf(rf.nextIdx[j]) >= rf.log.length() {
			// rf.nextIdx[j]--
			rf.nextIdx[j], _ = rf.log.lastIdxTerm()
			return
		}

		//rf.nextIdx[j]--

		originNextIdx := rf.nextIdx[j]
		// FastBackup
		rf.fastBackup(j, rf.posOf(rf.nextIdx[j]), reply.XTerm, reply.XIndex)
		rf.logger.Infof("Leader %d fastBackup: update nextIdx[%d] from %d to %d", rf.me, j, originNextIdx, rf.nextIdx[j])

		return
	}

	if len(args.Entries) == 0 {
		return
	}
	N := args.Entries[len(args.Entries) - 1].Index

	// follower agree, advanced follower's nextIdx and matchIdx
	// sentLastIdx := args.Entries
	if rf.nextIdx[j] < N + 1 {
		rf.nextIdx[j] = N + 1
	}
	if rf.matchIdx[j] < N {
		rf.matchIdx[j] = N
	}
	rf.logger.Debugf("Leader %d nextIdx[%d] update to %d", rf.me, j, rf.nextIdx[j])

	// if the majority agreed, commit entries, break loop
	if rf.majorityAgreeAt(N) {
		if N <= rf.log.CpIdx() {
			return
		}
		rf.logger.Infof("Leader %d majority agree with entry %d", rf.me, N)
		rf.logger.Infof("Leader %d N=%d commitIdx=%d currTerm=%d lastLogTerm=%d cpIdx=%d",
			rf.me, N, rf.commitIdx, rf.currTerm, rf.log.idxAt(N).Term, rf.log.CpIdx())

		if N > rf.commitIdx && rf.log.idxAt(N).Term == rf.currTerm {
			// majority of servers agreed, commit and apply the entries from oldCommitIdx+1 to N
			rf.commitIdx = N
			rf.log.commitTo(rf.commitIdx)
			rf.logger.Infof("Leader %d commitIdx update to %d", rf.me, N)
			rf.applyCond.Signal()
		}
	}
	return
}

func (rf *Raft) majorityAgreeAt(idx int) bool {
	cnt := 1
	for i := 0; i < rf.peers; i++ {
		if i == rf.me {
			continue
		}
		if rf.matchIdx[i] >= idx {
			cnt++
		}
	}
	return cnt == (rf.peers / 2 + 1)
}

func (rf *Raft) applyer() {
	for !rf.killed() {
		rf.mu.Lock()

		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIdx {
			rf.applyCond.Wait()
			if rf.killed() {
				break
			}
		}

		commitIndex, lastApplied := rf.commitIdx, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		if lastApplied == 0 {
			copy(entries, rf.log.slice(0, rf.posOf(rf.commitIdx)+1))
		} else {
			copy(entries, rf.log.slice(rf.posOf(rf.lastApplied)+1, rf.posOf(rf.commitIdx)+1))
		}

		rf.mu.Unlock()
		for _, entry := range entries {
			msg := ApplyMsg {
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
			rf.logger.Debugf("Peer %d ready to apply msg %d", rf.me, msg.CommandIndex)
			rf.applyCh <- msg
			rf.logger.Debugf("Peer %d applied msg %d", rf.me, msg.CommandIndex)
		}
		rf.mu.Lock()
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		if rf.lastApplied < commitIndex {
			rf.lastApplied = commitIndex
			rf.log.applyTo(rf.lastApplied)
			rf.logger.Debugf("Peer %d lastApplied advanced to %d", rf.me, rf.lastApplied)
		}

		rf.mu.Unlock()
	}
}

// j is the peer number, k is the posistion of nextIdx[j]
func (rf *Raft) fastBackup(j, k, xterm, xindex int) {
	rf.logger.Infof("Leader %d fastBackup follower %d, k=%d, xterm=%d, xindex=%d, cpIdx=%d", rf.me, j, k,
		xterm, xindex, rf.log.CpIdx())
	if xterm == -1 {
		// case 3
		//rf.nextIdx[j] -= reply.XLen
		rf.nextIdx[j] = xindex
	} else if xindex > rf.log.CpIdx() {
		// leader check whether contains any log entry of xterm
		for p := k + 1; p >= 1; p-- {
			if rf.log.posAt(p-1).Term == xterm {
				// case 2: leader contains xterm log, set nextIdx to p+1
				rf.nextIdx[j] = p + 1
				break
			}
			if rf.log.posAt(p-1).Term < xterm {
				// case1: leader dosn't contain xterm log, set nextIdx to XIndex
				rf.nextIdx[j] = xindex
				break
			}
			if p == 1 {
				// else: include in case1
				rf.nextIdx[j] = xindex
			}
		}
	}
	// need to send snapshot to help follower catch up
	if rf.nextIdx[j] <= rf.log.CpIdx() {
		rf.logger.Infof("Leader %d fastBackup follower %d need to send snapshot to help catch up", rf.me, j)
		snapshot := rf.persister.ReadSnapshot()
		rf.mu.Unlock()
		rf.doSendSnapshotToFollower(j, rf.log.CpIdx(), rf.log.CpTerm(), snapshot)
		rf.mu.Lock()
		return
	}
	if rf.nextIdx[j] == 0 {
		rf.nextIdx[j] = 1
	}
}

func (rf *Raft) eqMajority(cnt int) bool {
	return cnt == rf.peers/2+1
}

func (rf *Raft) getLastLogIdxTerm() (int, int) {
	if rf.log.length() == 0 {
		return 0, -1
	} else {
		lastLog := rf.log.last()
		return lastLog.Index, lastLog.Term
	}
}

func (rf *Raft) LogCompact(snapshot []byte, lastIncludedIdx int, needLock bool) {
	if needLock {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if lastIncludedIdx <= rf.log.CpIdx() {
			rf.logger.Debugf("Peer %d cpIdx=%d greater than lastIncludedIdx=%d no need to compact log",
				rf.me, rf.log.CpIdx(), lastIncludedIdx)
			return
		}
		if lastIncludedIdx > rf.lastApplied {
			rf.logger.Debugf("Peer %d lastIncludedIdx %d > lastApplied %d, advanced lastApplied", rf.me, lastIncludedIdx, rf.lastApplied)
			rf.lastApplied = lastIncludedIdx
		}
	}

	// lastLogIdx, _ := rf.getLastLogIdxTerm()
	// Assert(lastIncludedIdx > lastLogIdx, "lastIncludedIdx %d > lastLogIdx %d", lastIncludedIdx, lastLogIdx)

	// lastIncludedLog := rf.log.idxAt(lastIncludedIdx)

	rf.log.compactTo(lastIncludedIdx, -1, 0)

	if rf.logFileEnabled {
		rf.persister.SaveSnapshot(snapshot)
	} else {
		rf.persister.SaveStateAndSnapshot(rf.encodeRaftStateWithLog(), snapshot)
	}
	rf.logger.Infof("Peer %d LogCompact finish, lastIncludedIdx=%d", rf.me, lastIncludedIdx)
}

func (rf *Raft) doSendSnapshotToFollower(j, lastIncludedIdx, lastIncludedTerm int, snapshot []byte) {
	for {
		rf.mu.Lock()

		if rf.getRole() != RoleLeader {
			rf.mu.Unlock()
			return
		}

		args := InstallSnapshotArgs {
			RPCArgBase: &netw.RPCArgBase{},
			Term:             rf.currTerm,
			LeaderId:         rf.me,
			Data:             snapshot,
			LastIncludedIdx:  lastIncludedIdx,
			LastIncludedTerm: lastIncludedTerm,
			LastIncludedEndLSN: rf.log.CpLSN(),
		}
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()

		if ok := rf.sendInstallSnapshot(j, &args, &reply); !ok {
			rf.logger.Debugf("Leader %d failed to send InstallSnapshotRPC to follower %d wait to retry",
				rf.me, j)
			// time.Sleep(time.Millisecond * 100)
			// continue
			break
		}
		rf.mu.Lock()

		rf.logger.Infof("Leader %d sent InstallSnapshot RPC to follower %d", rf.me, j)

		if reply.Term > rf.currTerm {
			rf.whenHearBiggerTerm(reply.Term)
			rf.mu.Unlock()
			return
		}
		if rf.currTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// update nextIdx[j] to lastIncludedIdx
		rf.nextIdx[j] = lastIncludedIdx + 1
		rf.logger.Infof("Leader %d nextIdx[%d] update to %d because of InstallSnapshotRPC", rf.me, j, rf.nextIdx[j])
		rf.mu.Unlock()

		break
	}

}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

	for _, cond := range rf.replicatorCond {
		if cond != nil {
			cond.Signal()
		}
	}
	rf.applyCond.Broadcast()

}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// entry index => posistion (starts from 0)
func (rf *Raft) posOf(idx int) int {
	return idx - rf.log.CpIdx() - 1
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
			args := RequestVoteArgs {
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
						// go rf.Start(EmptyCmd{})
					})
				}
			}
		}(i)
	}
}

func (rf *Raft) reInitNextIdx() {
	if rf.log.length() != 0 {
		lastLogIdx := rf.log.last().Index
		for i := 0; i < rf.peers; i++ {
			rf.nextIdx[i] = lastLogIdx + 1
		}
	} else {
		for i := 0; i < rf.peers; i++ {
			rf.nextIdx[i] = rf.log.CpIdx() + 1
		}
	}
}