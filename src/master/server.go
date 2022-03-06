package master

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"mrkv/src/common"
	"mrkv/src/netw"
	"mrkv/src/raft"
)

type ShardMaster struct {
	mu      sync.RWMutex
	me      int
	servers []*netw.ClientEnd
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	opApplied   map[int]chan *OpApplyRes
	ckMaxSeq    map[int64]int64
	lastApplied int
	appliedCond *sync.Cond

	KilledC chan int

	log		*logrus.Logger
}

type SnapshotPackage struct {
	LastApplied int
	Configs          []Config
	CkMaxSeq    map[int64]int64
}

type Op interface {
	GetType() 	int
	GetSeq() 	int64
	GetCid()	int64
}

type OpBase struct {
	Type  	int
	Cid 	int64
	Seq 	int64
}

func (o *OpBase) GetType() int {
	return o.Type
}

func (o *OpBase) GetSeq() int64 {
	return o.Seq
}

func (o *OpBase) GetCid() int64 {
	return o.Cid
}

type OpJoinCmd struct {
	*OpBase
	Args JoinArgs
}

type OpLeaveCmd struct {
	*OpBase
	Args LeaveArgs
}

type OpMoveCmd struct {
	*OpBase
	Args MoveArgs
}

type OpQueryCmd struct {
	*OpBase
	Args QueryArgs
}


type OpApplyRes struct {
	op  Op
	res interface{}
	ok  bool
	idx int
}

func (sm *ShardMaster) applyOp(op Op, idx int) (res interface{}, ok bool) {

	var err error
	switch op.GetType() {
	case OpJoin:
		opJoin := op.(OpJoinCmd)
		res, err = sm.executeJoin(&opJoin.Args)
	case OpLeave:
		opLeave := op.(OpLeaveCmd)
		res, err = sm.executeLeave(&opLeave.Args)
	case OpMove:
		opMove := op.(OpMoveCmd)
		res, err = sm.executeMove(&opMove.Args)
	case OpQuery:
		opQuery := op.(OpQueryCmd)
		res, err = sm.executeQuery(&opQuery.Args)
	default:
		panic("unreconized op type")
	}
	sm.lastApplied = idx

	sm.log.Debugf("ShardMaster %d ready to apply op %d", sm.me, idx)
	return res, err == nil
}

func (sm *ShardMaster) applyer() {
	for {
		select {
		case msg := <-sm.applyCh:
			if !msg.CommandValid {
				continue
			}

			sm.mu.RLock()
			if msg.CommandIndex <= sm.lastApplied {
				sm.log.Debugf("ShardMaster %d Applyer msg idx(%d) less eq to lastApplied(%d)", sm.me, msg.CommandIndex, sm.lastApplied)
				sm.mu.RUnlock()
				continue
			}
			sm.mu.RUnlock()


			sm.mu.Lock()

			if _, ok := msg.Command.(raft.EmptyCmd); ok {
				sm.lastApplied = msg.CommandIndex
				sm.appliedCond.Broadcast()
				sm.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)
			var ok bool
			var reply interface{}
			if op.GetType() == OpJoin || op.GetType() == OpLeave || op.GetType() == OpMove {
				maxSeq, ok1 := sm.ckMaxSeq[op.GetCid()]
				if !ok1 || op.GetSeq() > maxSeq {
					reply, ok = sm.applyOp(op, msg.CommandIndex)

					sm.ckMaxSeq[op.GetCid()] = op.GetSeq()
				}
			} else {
				reply, ok = sm.applyOp(op, msg.CommandIndex)
			}
			res := &OpApplyRes {
				op:  op,
				res: reply,
				ok:  ok,
				idx: msg.CommandIndex,
			}
			// DPrintf("ShardMaster %d Applyer apply op %d, res is %v", sm.me, msg.CommandIndex, res.res)

			sm.lastApplied = msg.CommandIndex
			sm.appliedCond.Broadcast()

			waitC := sm.getWaitCh(res.idx)
			sm.mu.Unlock()

			sm.log.Debugf("ShardMaster %d Applyer ready to send msg %v to wait channel %d", sm.me, msg.CommandIndex, res.idx)
			select {
			case waitC <- res:
				sm.log.Debugf("ShardMaster %d Applyer send msg %v to wait channel %d", sm.me, msg.CommandIndex, res.idx)
			default:
			}

		case <-sm.KilledC:
			sm.log.Infof("ShardMaster %d has been killed, stop applyer loop", sm.me)
			return
		}
	}
}

func (sm *ShardMaster) raftStart(op Op) (res interface{}, ok bool, err string) {
	var idx, term int
	var isLeader bool

	if idx, term, isLeader = sm.rf.Start(op); !isLeader {
		return nil, false, ErrWrongLeader
	}
	sm.log.Debugf("ShardMaster %d call raft.start res: idx=%d term=%d isLeader=%v", sm.me, idx, term, isLeader)
	//	fmt.Printf("op: %v", op)

	sm.mu.Lock()
	waitC := sm.getWaitCh(idx)
	sm.mu.Unlock()

	defer func() {
		go sm.delWaitChLock(idx)
	}()

	// wait for being applied
	select {
	case opRes := <-waitC:
		if op.GetType() != opRes.op.GetType() || op.GetSeq() != opRes.op.GetSeq() || op.GetCid() != opRes.op.GetCid() {
			sm.log.Warnf("ShardMaster %d apply command not match to the original wanted(%v) != actually(%v)",
				sm.me, op, opRes.op)
			return nil, false, ErrFailed
		}
		sm.log.Debugf("ShardMaster %d receive res %v from waiting channel %v", sm.me, opRes.res, opRes.idx)
		return opRes.res, opRes.ok, OK

	case <-time.After(time.Second * 1):
		sm.log.Warnf("ShardMaster %d op %v at idx %d has not commited after 1 secs", sm.me, op.GetSeq(), idx)
		return nil, false, ErrFailed
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) (e error)  {
	op := OpJoinCmd {
		OpBase: &OpBase{
			Type:  OpJoin,
			Cid:   args.Cid,
			Seq:   args.Seq,
		},
		Args: *args,
	}
	sm.log.Infof("ShardMaster %d receive join command args: %v", sm.me, args)

	_, _, err := sm.raftStart(op)
	reply.Err = Err(err)
	reply.WrongLeader = (err == ErrWrongLeader)

	sm.log.Infof("ShardMaster %d join command reply: %v", sm.me, *reply)
	sm.printLatestConfig(true)

	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) (e error) {
	op := OpLeaveCmd {
		OpBase: &OpBase{
			Type:  OpLeave,
			Cid:   args.Cid,
			Seq:   args.Seq,
		},
		Args: *args,
	}
	sm.log.Infof("ShardMaster %d receive leave command args: %v", sm.me, args)

	_, _, err := sm.raftStart(op)
	reply.Err = Err(err)
	reply.WrongLeader = (err == ErrWrongLeader)

	sm.log.Infof("ShardMaster %d leave command reply: %v", sm.me, *reply)
	sm.printLatestConfig(true)

	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) (e error)  {
	op := OpMoveCmd{
		OpBase: &OpBase{
			Type:  OpMove,
			Cid:   args.Cid,
			Seq:   args.Seq,
		},
		Args: *args,
	}
	sm.log.Infof("ShardMaster %d receive move command args: %v", sm.me, args)

	_, _, err := sm.raftStart(op)
	reply.Err = Err(err)
	reply.WrongLeader = (err == ErrWrongLeader)

	sm.log.Infof("ShardMaster %d move command reply: %v", sm.me, *reply)
	sm.printLatestConfig(true)

	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) (e error) {
	op := OpQueryCmd {
		OpBase: &OpBase{
			Type:  OpQuery,
			Cid:   args.Cid,
			Seq:   args.Seq,
		},
		Args: *args,
	}
	sm.log.Debugf("ShardMaster %d receive query command args: %v", sm.me, *args)

	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sm.mu.RLock()
	if args.Num >= 0 && args.Num < len(sm.configs) {
		reply.Config = sm.configs[args.Num]
		sm.mu.RUnlock()
		return
	}
	sm.mu.RUnlock()

	if ok, readIdx := sm.rf.ReadIndex(); ok {
		sm.log.Debugf("ShardMaster %d query ReadIndex %d start to wait", sm.me, readIdx)
		sm.waitAppliedTo(readIdx)
		sm.log.Debugf("ShardMaster %d query ReadIndex %d success", sm.me, readIdx)

		sm.mu.RLock()
		reply.Config = sm.configs[len(sm.configs)-1]
		sm.mu.RUnlock()
		return
	}

	res, _, err := sm.raftStart(op)
	if err != OK {
		reply.Err = Err(err)
		reply.WrongLeader = err == ErrWrongLeader
	} else {
		reply.Err = OK
		r, _ := res.(*QueryReply)
		reply.Config = r.Config
	}
	sm.log.Debugf("ShardMaster %d query command reply: %v", sm.me, *reply)
	// sm.printLatestConfig(true)

	return
}

func (sm *ShardMaster) executeJoin(args *JoinArgs) (reply *JoinReply, err error) {

	latestConfig := sm.getLatestConfig()

	// copy the groups map
	groups := make(map[int][]string)
	for gid, servers := range latestConfig.Groups {
		groups[gid] = servers
	}

	// copy the shards
	shards := [NShards]int{}
	for i, v := range latestConfig.Shards {
		shards[i] = v
	}

	// join
	for gid, servers := range args.Servers {
		groups[gid] = servers
	}

	newConfig := Config {
		Num:    latestConfig.Num + 1,
		Groups: groups,
		Shards: shards,
	}

	sm.configs = append(sm.configs, newConfig)

	// re-balance
	sm.rebalanced()

	return &JoinReply {
		Err: OK,
		WrongLeader: false,
	}, nil
}

func (sm *ShardMaster) executeLeave(args *LeaveArgs) (reply *LeaveReply, err error) {

	latestConfig := sm.getLatestConfig()

	leaveGidMap := make(map[int]bool)
	for _, gid := range args.GIDs {
		leaveGidMap[gid] = true
	}

	// copy and remove
	groups := make(map[int][]string)
	for gid, servers := range latestConfig.Groups {
		if _, ok := leaveGidMap[gid]; !ok {
			groups[gid] = servers
		}
	}
	shards := [NShards]int{}
	for i := 0; i < len(shards); i++ {
		if _, ok := leaveGidMap[latestConfig.Shards[i]]; !ok {
			shards[i] = latestConfig.Shards[i]
		} else {
			shards[i] = 0
		}
	}
	newConfig := Config {
		Num:    latestConfig.Num + 1,
		Groups: groups,
		Shards: shards,
	}
	sm.configs = append(sm.configs, newConfig)

	// rebalanced (shards of left groups should move to present groups)
	sm.rebalanced()

	return &LeaveReply {
		Err: OK,
		WrongLeader: false,
	}, nil
}

func (sm *ShardMaster) executeMove(args *MoveArgs) (reply *MoveReply, err error) {

	latestConfig := sm.getLatestConfig()

	// copy the groups map
	groups := make(map[int][]string)
	for gid, servers := range latestConfig.Groups {
		groups[gid] = servers
	}

	// copy the shards
	shards := [NShards]int{}
	for i, v := range latestConfig.Shards {
		shards[i] = v
	}

	// move
	fromGid := latestConfig.Shards[args.Shard]
	toGid := args.GID
	shards[args.Shard] = toGid

	newConfig := Config {
		Num:    latestConfig.Num + 1,
		Groups: groups,
		Shards: shards,
	}
	sm.configs = append(sm.configs, newConfig)

	sm.log.Infof("ShardMaster %d execute move: move shard %d from group %d to group %d", sm.me, args.Shard, fromGid, toGid)

	return &MoveReply {
		Err:         OK,
		WrongLeader: false,
	}, nil
}

func (sm *ShardMaster) getLatestConfig() Config {
	latestConfig := sm.configs[len(sm.configs)-1]
	return latestConfig
}

func (sm *ShardMaster) executeQuery(args *QueryArgs) (reply *QueryReply, err error) {
	var config Config
	if args.Num == -1 || args.Num >= len(sm.configs) {
		config = sm.getLatestConfig()
	} else {
		config = sm.configs[args.Num]
	}
	return &QueryReply {
		Err: OK,
		WrongLeader: false,
		Config: config,
	}, nil
}

func (sm *ShardMaster) rebalanced() {
	latestConfig := sm.configs[len(sm.configs)-1]
	sm.log.Infof("ShardMaster %d begin rebalanced,current config: %v", sm.me, latestConfig)

	if len(latestConfig.Groups) == 0 {
		sm.log.Infof("ShardMaster %d rebalanced: zero group, do nothing", sm.me)
		return
	} else if len(latestConfig.Groups) == 1 {
		// if only one group, assign all shards to it
		toGid := 0
		for gid, _ := range latestConfig.Groups {
			toGid = gid
			break
		}
		for i := 0; i < len(latestConfig.Shards); i++ {
			latestConfig.Shards[i] = toGid
		}
		sm.configs[len(sm.configs)-1] = latestConfig

		sm.log.Infof("ShardMaster %d rebalanced: only one group %d, assign all", sm.me ,toGid)
		return
	}

	numOfShards := make(map[int]int)
	for gid, _ := range latestConfig.Groups {
		numOfShards[gid] = 0
	}
	for _, gid := range latestConfig.Shards {
		numOfShards[gid]++
	}
	sm.log.Debugf("ShardMaster %d rebalanced: numOfShards: %v", sm.me ,numOfShards)

	gidsOrderByNumShards := make([]int,0)
	for gid, _ := range numOfShards {
		gidsOrderByNumShards = append(gidsOrderByNumShards, gid)
	}
	sort.Slice(gidsOrderByNumShards, func(i, j int) bool {
		return numOfShards[gidsOrderByNumShards[i]] < numOfShards[gidsOrderByNumShards[j]]
	})
	sm.log.Debugf("ShardMaster %d rebalanced: gidsOrderByNumShards: %v", sm.me, gidsOrderByNumShards)

	numGroups := len(latestConfig.Groups)
	avg := NShards / numGroups
	numPlusOne := NShards % numGroups

	for i := len(gidsOrderByNumShards) - 1; i >= 0; i-- {
		fromGid := gidsOrderByNumShards[i]
		if numOfShards[fromGid] <= avg {
			break
		}
		target := avg
		if numPlusOne > 0 && fromGid != 0 {
			target = avg + 1
			numPlusOne--
		} else {
			target = avg
		}
		delta := numOfShards[fromGid] - target
		for j, gid := range latestConfig.Shards {
			if delta <= 0 {
				break
			}
			if gid == fromGid {
				latestConfig.Shards[j] = 0
				delta--
			}
		}
		numOfShards[fromGid] = target
	}

	for i := 0; i < len(gidsOrderByNumShards); i++ {
		toGid := gidsOrderByNumShards[i]
		if numOfShards[toGid] >= avg {
			break
		}
		target := 0
		if numPlusOne > 0 {
			target = avg + 1
			numPlusOne--
		} else {
			target = avg
		}
		delta := target - numOfShards[toGid]
		for j, gid := range latestConfig.Shards {
			if delta <= 0 {
				break
			}
			if gid == 0 {
				latestConfig.Shards[j] = toGid
				delta--
			}
		}
		numOfShards[toGid] = target
	}

	sm.configs[len(sm.configs)-1] = latestConfig

	sm.log.Infof("ShardMaster %d rebalanced: finished: %v", sm.me, latestConfig)
}

func (sm *ShardMaster) printLatestConfig(needLock bool)  {
	if needLock {
		sm.mu.RLock()
	}
	c := sm.getLatestConfig()
	if needLock {
		sm.mu.RUnlock()
	}

	gid2Shards := make(map[int][]int)
	for s, gid := range c.Shards {
		if _, ok := gid2Shards[gid]; !ok {
			gid2Shards[gid] = make([]int, 0)
		}
		gid2Shards[gid] = append(gid2Shards[gid], s)
	}
	for gid, shards := range gid2Shards {
		sm.log.Infof("%d -> %v", gid, shards)
	}
}

func (sm *ShardMaster) getWaitCh(idx int) chan *OpApplyRes {
	ch, ok := sm.opApplied[idx]
	if !ok {
		ch = make(chan *OpApplyRes, 1)
		sm.opApplied[idx] = ch
	}
	return ch
}

func (sm *ShardMaster) delWaitCh(idx int) {
	delete(sm.opApplied, idx)
}

func (sm *ShardMaster) delWaitChLock(idx int) {
	sm.mu.Lock()
	delete(sm.opApplied, idx)
	sm.mu.Unlock()
}

func (sm *ShardMaster) waitAppliedTo(target int) {
	sm.appliedCond.L.Lock()
	for sm.lastApplied < target {
		sm.appliedCond.Wait()
	}
	sm.appliedCond.L.Unlock()
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.KilledC <- 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}


func (sm *ShardMaster) StartRPCServer() error {
	if err := rpc.RegisterName(fmt.Sprintf("Master%d", sm.me) ,sm); err != nil {
		return err
	}
	l, err := net.Listen(sm.servers[sm.me].Network, sm.servers[sm.me].Addr)
	if err != nil {
		return err
	}
	go func() {
		if err := http.Serve(l, nil); err != nil {
			panic(err)
		}

	}()
	return nil
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant master service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*netw.ClientEnd, me int, rf *raft.Raft, ch chan raft.ApplyMsg, logLevel string) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.opApplied = make(map[int]chan *OpApplyRes)
	sm.appliedCond = sync.NewCond(&sync.Mutex{})
	sm.ckMaxSeq = make(map[int64]int64)
	sm.KilledC = make(chan int, 1)

	sm.rf = rf
	sm.applyCh = ch
	sm.servers = servers

	sm.log, _ = common.InitLogger(logLevel, fmt.Sprintf("Master%d", sm.me))

	go sm.applyer()

	return sm
}
