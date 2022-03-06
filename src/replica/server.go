package replica

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"

	"mrkv/src/common"
	"mrkv/src/common/labgob"
	"mrkv/src/master"
	"mrkv/src/netw"
	"mrkv/src/raft"
)

var LogFileNameByOS = map[string]string{
	"linux": "/root/raftlogs/shardkv/%d/logfile%d",
	"windows": "C:\\Users\\83780\\Desktop\\raftlogs\\shardkv\\%d\\logfile%d",
}

type ShardKV struct {
	log			 *logrus.Logger
	
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg

	servers		 []*netw.ClientEnd
	ends		 map[int][]*netw.ClientEnd

	gid          int
	masters      []*netw.ClientEnd
	mck			 *master.Clerk
	maxraftstate int
	persister	 *raft.MemoryPersister

	opApplied   map[int]chan ApplyRes
	lastApplied int
	appliedCond *sync.Cond

	store      Store
	shardDB    map[int]*Shard
	currConfig master.Config
	prevConfig master.Config

	KilledC chan int
	exitedC chan string
	dead    int32 // set by Kill()

	numDaemon int
}

func (kv *ShardKV) SetCurrConfig(config master.Config)  {
	buf := new(bytes.Buffer)
	if err := labgob.NewEncoder(buf).Encode(config); err != nil {
		panic(err)
	}
	if err := kv.store.Put(KeyCurrConfig, buf.Bytes()); err != nil {
		panic(err)
	}
	kv.currConfig = config

}

func (kv *ShardKV) SetPrevConfig(config master.Config) {
	buf := new(bytes.Buffer)
	if err := labgob.NewEncoder(buf).Encode(config); err != nil {
		panic(err)
	}
	if err := kv.store.Put(KeyPrevConfig, buf.Bytes()); err != nil {
		panic(err)
	}
	kv.prevConfig = config
}

func (kv *ShardKV) SetLastApplied(lastApplied int) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(lastApplied))
	if err := kv.store.Put(KeyLastApplied, buf); err != nil {
		panic(err)
	}
	kv.lastApplied = lastApplied
}

func (kv *ShardKV) RecoverFromStore() (err error) {
	var buf []byte

	if buf, err = kv.store.Get(KeyLastApplied); err != nil {
		return
	} else {
		kv.lastApplied = int(binary.LittleEndian.Uint64(buf))
	}
	if buf, err = kv.store.Get(KeyCurrConfig); err != nil {
		return
	} else {
		if err = labgob.NewDecoder(bytes.NewReader(buf)).Decode(&kv.currConfig); err != nil {
			return
		}
	}
	if buf, err = kv.store.Get(KeyPrevConfig); err != nil {
		return
	} else {
		if err = labgob.NewDecoder(bytes.NewReader(buf)).Decode(&kv.currConfig); err != nil {
			return
		}
	}

	for i := 0; i < master.NShards; i++ {
		kv.shardDB[i] = MakeShard(i, INVALID, 0, kv.store)

	}


	return
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) (e error) {
	defer func() {
		if err := recover(); err != nil {
			if err == leveldb.ErrClosed {
				return
			}
			kv.log.Fatalln(err)
		}
	}()

	cmd := KVCmd {
		CmdBase: &CmdBase{
			Type: CmdKV,
			Cid: args.Cid,
			Seq: args.Seq,
		},
		Op: Op{
			Type: OpGet,
			Key:  args.Key,
		},
	}
	kv.log.Infof("KVServer %d receive op: %v", kv.me, cmd.Op)

	if !kv.checkShardInCharge(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	if ok, readIdx := kv.rf.ReadIndex(); ok {
		kv.log.Debugf("KVServer %d ReadIndex %d start to wait", kv.me, readIdx)
		kv.waitAppliedTo(readIdx)
		kv.log.Debugf("KVServer %d ReadIndex %d success", kv.me, readIdx)

		kv.mu.RLock()
		reply.Value, reply.Err = kv.executeGet(args.Key)
		kv.mu.RUnlock()
		return
	}

	if res, err := kv.raftStartCmdWait(cmd); err != OK {
		reply.Err = err
	} else {
		reply.Err = res.GetErr()
		reply.Value = res.(*KVCmdApplyRes).val
		kv.log.Infof("KVServer %d finished GET, key=%s, shard=%d, val=%s",kv.me, args.Key, key2shard(args.Key), reply.Value)
	}

	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) (e error) {
	defer func() {
		if err := recover(); err != nil {
			if err == leveldb.ErrClosed {
				return
			}
			log.Panicln(err)
		}
	}()


	cmd := KVCmd {
		CmdBase: &CmdBase{
			Type: CmdKV,
			Cid: args.Cid,
			Seq: args.Seq,
		},
		Op: Op{
			Type: args.Op,
			Key:  args.Key,
			Value: args.Value,
		},
	}
	kv.log.Infof("KVServer %d receive cmd: %v, cid=%d, seq=%d", kv.me, cmd, cmd.Cid, cmd.Seq)

	if !kv.checkShardInCharge(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	if kv.isDuplicated(args.Key, args.Cid, args.Seq) {
		reply.Err = ErrDuplicate
		return
	}

	if res, err := kv.raftStartCmdWait(cmd); err != OK {
		reply.Err = err
	} else {
		reply.Err = res.GetErr()
		kv.log.Infof("KVServer %d finished PUTAPPEND, key=%s, shard=%d, val=%s",kv.me, args.Key, key2shard(args.Key), args.Value)
	}

	return
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) (e error) {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	kv.log.Infof("KVServer %d receive PullShard: %v", kv.me, *args)

	if kv.currConfig.Num < args.ConfNum {
 		reply.Err = ErrDiffConf
		reply.ConfNum = kv.currConfig.Num
		return
	}

	reply.Shards = make(map[int][]byte)
	for _, id := range args.Shards {
		shard := kv.shardDB[id]
		reply.Shards[id] = shard.Dump()
	}

	reply.ConfNum = kv.currConfig.Num
	reply.Err = OK

	return
}

func (kv *ShardKV) EraseShard(args *EraseShardArgs, reply *EraseShardReply ) (e error) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.log.Infof("KVServer %d receive EraseShard: %v", kv.me, *args)

	kv.mu.RLock()
	if kv.currConfig.Num > args.ConfNum {
		reply.ConfNum = kv.currConfig.Num
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}
	if kv.currConfig.Num != args.ConfNum {
		reply.Err = ErrDiffConf
		reply.ConfNum = kv.currConfig.Num
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	cmd := EraseShardCmd {
		CmdBase: &CmdBase{
			Type: CmdEraseShard,
		},
		Shards: args.Shards,
		ConfNum: kv.currConfig.Num,
	}
	res, err := kv.raftStartCmdWait(cmd)
	if err != OK {
		reply.Err = err
		reply.ConfNum = kv.currConfig.Num
		return
	}
	reply.Err = res.GetErr()
	reply.ConfNum = kv.currConfig.Num

	return
}

func (kv *ShardKV) checkShardInCharge(key string) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	shardId := key2shard(key)
	if kv.currConfig.Shards[shardId] != kv.gid {
		return false
	}
	if shard, ok := kv.shardDB[shardId]; !ok || !(shard.Status == SERVING || shard.Status == WAITING) {
		return false
	}
	return true
}

func (kv *ShardKV) raftStartCmdNoWait(cmd Cmd) Err {
	var idx, term int
	var isLeader bool

	if idx, term, isLeader = kv.rf.Start(cmd); !isLeader {
		return ErrWrongLeader
	}
	kv.log.Debugf("KVServer %d call raft.start res: Idx=%d term=%d isLeader=%v cmd=%v", kv.me, idx, term, isLeader, cmd.GetType())

	return OK
}

func (kv *ShardKV) raftStartCmdWait(cmd Cmd) (ApplyRes, Err) {
	var idx, term int
	var isLeader bool

	if idx, term, isLeader = kv.rf.Start(cmd); !isLeader {
		return nil, ErrWrongLeader
	}
	kv.log.Debugf("Group %d KVServer %d call raft.start res: Idx=%d term=%d isLeader=%v cmd=%v", kv.gid, kv.me, idx, term, isLeader, cmd)

	kv.mu.Lock()
	waitC := kv.getWaitCh(idx)
	kv.mu.Unlock()
	defer kv.delWaitChLock(idx)

	kv.log.Debugf("KVServer %d waiting at the channel %v, idx=%d, cmd.cid=%d, cmd.seq=%d", kv.me, waitC, idx, cmd.GetCid(), cmd.GetSeq())
	// wait for being applied
	select {
	case res := <-waitC:
		if res.GetCmdType() != cmd.GetType()  {
			kv.log.Debugf("KVServer %d apply command not match to the original wanted(%v) != actually(%v)",
				kv.me, cmd, res)
			return nil, ErrFailed
		}
		kv.log.Debugf("KVServer %d receive res from waiting channel %v", kv.me, res.GetIdx())
		return res, OK

	case <-time.After(time.Second * 1):
		kv.log.Warnf("KVServer %d op %v at Idx %d has not commited after 1 secs", kv.me, cmd.GetSeq(), idx)
		return nil, ErrFailed
	}
}

func (kv *ShardKV) getWaitCh(idx int) chan ApplyRes {
	ch, ok := kv.opApplied[idx]
	if !ok {
		ch = make(chan ApplyRes, 1)
		kv.opApplied[idx] = ch
	}
	return ch
}

func (kv *ShardKV) delWaitCh(idx int) {
	delete(kv.opApplied, idx)
}

func (kv *ShardKV) delWaitChLock(idx int) {
	kv.mu.Lock()
	delete(kv.opApplied, idx)
	kv.mu.Unlock()
}

func (kv *ShardKV) waitAppliedTo(target int) {
	kv.appliedCond.L.Lock()
	for kv.lastApplied < target {
		kv.appliedCond.Wait()
	}
	kv.appliedCond.L.Unlock()
}

func (kv *ShardKV) isDuplicated(key string, cid, seq int64) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	shardId := key2shard(key)
	shard := kv.shardDB[shardId]
	return shard.IfDuplicateAndSet(cid, seq, false)
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)

	for i := 0; i < kv.numDaemon; i++ {
		kv.KilledC <- i
	}
	for i := 0; i < kv.numDaemon; i++ {
		name := <-kv.exitedC
		kv.log.Warnf("KVServer %d daemon function %s exited", kv.me, name)
	}
	kv.store.Close()

	kv.log.Warnf("KVServer %d exited", kv.me)

}

func (kv *ShardKV) Killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) StartRPCServer() error {
	if err := rpc.RegisterName(fmt.Sprintf("Replica-%d-%d", kv.gid, kv.me) , kv); err != nil {
		return err
	}
	l, err := net.Listen(kv.servers[kv.me].Network, kv.servers[kv.me].Addr)
	if err != nil {
		return err
	}
	go http.Serve(l, nil)
	return nil
}

func StartServer(rf *raft.Raft, ch chan raft.ApplyMsg, servers []*netw.ClientEnd, me int, persister *raft.MemoryPersister,
	gid int, masters []*netw.ClientEnd, dbPath string, logLevel string) *ShardKV {

	kv := new(ShardKV)
	
	kv.log, _ = common.InitLogger(logLevel, fmt.Sprintf("Replica-%d-%d", gid, me))
	
	kv.me = me
	kv.gid = gid
	kv.masters = masters
	kv.servers = servers
	kv.ends = make(map[int][]*netw.ClientEnd)

	kv.mck = master.MakeClerk(kv.masters)
	kv.persister = persister

	store, err := MakeLevelStore(dbPath)
	if err != nil {
		panic(err)
	}
	kv.store = store

	kv.rf = rf
	kv.applyCh = ch

	kv.opApplied = make(map[int]chan ApplyRes)
	kv.appliedCond = sync.NewCond(&kv.mu)

	kv.numDaemon = 5

	kv.KilledC = make(chan int, kv.numDaemon + 1)
	kv.exitedC = make(chan string, kv.numDaemon)

	kv.shardDB = make(map[int]*Shard)

	kv.currConfig = master.Config{
		Num: 0,
		Shards: [master.NShards]int{},
		Groups: map[int][]string{},
	}

	// restore from snapshot
	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		kv.applySnapshot(snapshot)
		kv.log.Infof("KVServer %d restore from snapshot, size %d", me, len(snapshot))
	} else {
		if err = store.Clear(""); err != nil {
			panic(err)
		}
		kv.log.Infof("KVServer %d no snapshot or empty snapshot to restore", me)
	}

	for i := 0; i < master.NShards; i++ {
		if _, ok := kv.shardDB[i]; !ok {
			kv.shardDB[i] = MakeShard(i, INVALID, 0, store)
		}
	}

	go kv.checkpointer()
	go kv.applyer()
	go kv.confUpdater()
	go kv.shardPuller()
	go kv.shardEraser()

	return kv
}
