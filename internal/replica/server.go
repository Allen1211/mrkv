package replica

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/Allen1211/mrkv/internal/netw"
	"github.com/Allen1211/mrkv/internal/raft"
	"github.com/Allen1211/mrkv/pkg/common"
	"github.com/Allen1211/mrkv/pkg/common/labgob"
	"github.com/Allen1211/mrkv/pkg/common/utils"
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

	gid          int
	persister	 *raft.DiskPersister

	opApplied   map[int]chan ApplyRes
	lastApplied int
	appliedCond *sync.Cond

	store      Store
	shardDB    map[int]*Shard
	currConfig common.ConfigV1
	prevConfig common.ConfigV1

	KilledC chan int
	exitedC chan string
	dead    int32 // set by Kill()

	numDaemon int

	rpcFunc netw.RpcFunc
}

func (kv*ShardKV) Raft() *raft.Raft {
	return kv.rf
}

func (kv *ShardKV) Me() int {
	return kv.me
}

func (kv *ShardKV) SetCurrConfig(config common.ConfigV1)  {
	buf := new(bytes.Buffer)
	if err := labgob.NewEncoder(buf).Encode(config); err != nil {
		kv.log.Errorln(err)
	}
	if err := kv.store.Put(fmt.Sprintf(KeyCurrConfig, kv.gid), buf.Bytes()); err != nil {
		kv.log.Errorln(err)
	}
	kv.currConfig = config

}

func (kv *ShardKV) GetCurrConfig() common.ConfigV1 {
	kv.mu.RLock()
	res := kv.currConfig
	kv.mu.RUnlock()
	return res
}

func (kv *ShardKV) SetPrevConfig(config common.ConfigV1) {
	buf := new(bytes.Buffer)
	if err := labgob.NewEncoder(buf).Encode(config); err != nil {
		kv.log.Errorln(err)
	}
	if err := kv.store.Put(fmt.Sprintf(KeyPrevConfig, kv.gid), buf.Bytes()); err != nil {
		kv.log.Errorln(err)
	}
	kv.prevConfig = config
}

func (kv *ShardKV) SetLastApplied(lastApplied int) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(lastApplied))
	if err := kv.store.Put(fmt.Sprintf(KeyLastApplied, kv.gid), buf); err != nil {
		kv.log.Errorln(err)
	}
	kv.lastApplied = lastApplied
}

func (kv *ShardKV) SetLastAppliedBatch(lastApplied int, batch Batch) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(lastApplied))
	batch.Put(fmt.Sprintf(KeyLastApplied, kv.gid), buf)
	kv.lastApplied = lastApplied
	kv.appliedCond.Broadcast()
}

func (kv *ShardKV) Clear() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.rf.Clear()

	prefix := fmt.Sprintf(KeyReplicaPrefix, kv.gid)
	if err := kv.store.Clear(prefix); err != nil {
		kv.log.Fatalf("failed to clear store: %v", err)
	}
	kv.store.Close()
	kv.store.DeleteFile()
}

func (kv *ShardKV) GetGroupInfo() *common.GroupInfo {

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	// _ = kv.store.Sync()

	_, isLeader := kv.rf.GetState()
	conf := kv.currConfig
	sizeOfGroup := int64(0)

	sizeOfGroup = kv.store.FileSize()

	shards := make(map[int]common.ShardInfo)
	for id, shard := range kv.shardDB {
		shards[id] = common.ShardInfo{
			Id: 	id,
			Gid: 	conf.Shards[id],
			Status: shard.Status,
			Size:   shard.Size(),
			ExOwner: shard.ExOwner,
		}
		// sizeOfGroup += shards[id].Size
	}
	// prefix := fmt.Sprintf(KeyReplicaPrefix, kv.gid)
	// if size, err := kv.store.Size([]string{prefix}); err != nil {
	// 	kv.log.Errorln(err)
	// } else {
	// 	sizeOfGroup += size
	// }

	res := &common.GroupInfo{
		Id: 		kv.gid,
		IsLeader:   isLeader,
		ConfNum:    conf.Num,
		Shards: 	shards,
		Size: 		sizeOfGroup,
		Peer:     	kv.me,
	}

	return res
}

func (kv *ShardKV) RecoverFromStore() (err error) {
	var buf []byte

	if buf, err = kv.store.Get(fmt.Sprintf(KeyLastApplied, kv.gid)); err != nil {
		return
	} else if buf != nil {
		kv.lastApplied = int(binary.LittleEndian.Uint64(buf))
	}
	if buf, err = kv.store.Get(fmt.Sprintf(KeyCurrConfig, kv.gid)); err != nil {
		return
	} else if buf != nil {
		if err = labgob.NewDecoder(bytes.NewReader(buf)).Decode(&kv.currConfig); err != nil {
			return
		}
	}
	if buf, err = kv.store.Get(fmt.Sprintf(KeyPrevConfig, kv.gid)); err != nil {
		return
	} else if buf != nil {
		if err = labgob.NewDecoder(bytes.NewReader(buf)).Decode(&kv.prevConfig); err != nil {
			return
		}
	}

	for i := 0; i < common.NShards; i++ {
		kv.shardDB[i] = &Shard{
			Idx:     i,
			ExOwner: 0,
			Status:  common.INVALID,
			Store:   kv.store,
		}
		if err := kv.shardDB[i].LoadFromStore(); err != nil {
			kv.log.Fatalf("shard %d cannot load from store: %v", i, err)
		}
	}

	return
}


func (kv *ShardKV) Get(args *common.GetArgs, reply *common.GetReply) (e error) {

	cmd := KVCmd{
		CmdBase: CmdBase{
			Cid: args.Cid,
			Seq: args.Seq,
		},
		Op: Op{
			Type: common.OpGet,
			Key:  args.Key,
		},
	}
	kv.log.Infof("KVServer %d receive op: %v", kv.me, cmd.Op)

	if !kv.checkShardInCharge(args.Key) {
		reply.Err = common.ErrWrongGroup
		return
	}

	if ok, readIdx := kv.rf.ReadIndex(); ok {
		kv.log.Debugf("KVServer %d ReadIndex %d start to wait", kv.me, readIdx)
		kv.waitAppliedTo(readIdx)
		kv.log.Debugf("KVServer %d ReadIndex %d success", kv.me, readIdx)
		if kv.Killed() {
			reply.Err = common.ErrNodeClosed
			return
		}
		kv.mu.RLock()
		reply.Value, reply.Err = kv.executeGet(args.Key)
		reply.Peer, reply.GID = kv.me, kv.gid
		kv.mu.RUnlock()
		return
	}
	if res, err := kv.raftStartCmdWait(common.CmdTypeKV, utils.MsgpEncode(&cmd)); err != common.OK {
		reply.Err = err
	} else {
		reply.Err = res.GetErr()
		reply.Value = res.(*KVCmdApplyRes).val
		kv.log.Infof("KVServer %d finished GET, key=%s, shard=%d, val=%s",kv.me, args.Key, common.Key2shard(args.Key), reply.Value)
	}
	reply.Peer, reply.GID = kv.me, kv.gid

	return
}

func (kv *ShardKV) PutAppend(args *common.PutAppendArgs, reply *common.PutAppendReply) (e error) {

	cmd := KVCmd{
		CmdBase: CmdBase{
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
		reply.Err = common.ErrWrongGroup
		return
	}
	// if kv.isDuplicated(args.Key, args.Cid, args.Seq) {
	// 	reply.Err = common.ErrDuplicate
	// 	return
	// }

	if res, err := kv.raftStartCmdWait(common.CmdTypeKV, utils.MsgpEncode(&cmd)); err != common.OK {
		reply.Err = err
	} else {
		reply.Err = res.GetErr()
		kv.log.Infof("KVServer %d finished PUTAPPEND, key=%s, shard=%d, val=%s",kv.me, args.Key, common.Key2shard(args.Key), args.Value)
	}
	reply.Peer, reply.GID = kv.me, kv.gid

	return
}

func (kv *ShardKV) Delete(args *common.DeleteArgs, reply *common.DeleteReply) (e error) {

	cmd := KVCmd{
		CmdBase: CmdBase{
			Cid: args.Cid,
			Seq: args.Seq,
		},
		Op: Op{
			Type: common.OpDelete,
			Key:  args.Key,
		},
	}
	kv.log.Infof("KVServer %d receive cmd: %v, cid=%d, seq=%d", kv.me, cmd, cmd.Cid, cmd.Seq)

	if !kv.checkShardInCharge(args.Key) {
		reply.Err = common.ErrWrongGroup
		return
	}
	// if kv.isDuplicated(args.Key, args.Cid, args.Seq) {
	// 	reply.Err = common.ErrDuplicate
	// 	return
	// }

	if res, err := kv.raftStartCmdWait(common.CmdTypeKV, utils.MsgpEncode(&cmd)); err != common.OK {
		reply.Err = err
	} else {
		reply.Err = res.GetErr()
		kv.log.Infof("KVServer %d finished DELETE, key=%s, shard=%d",kv.me, args.Key, common.Key2shard(args.Key))
	}
	reply.Peer, reply.GID = kv.me, kv.gid

	return
}

func (kv *ShardKV) PullShard(args *common.PullShardArgs, reply *common.PullShardReply) (e error) {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = common.ErrWrongLeader
		return
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	kv.log.Infof("KVServer %d receive PullShard: %v", kv.me, *args)

	if kv.currConfig.Num < args.ConfNum {
 		reply.Err = common.ErrDiffConf
		reply.ConfNum = kv.currConfig.Num
		return
	}

	reply.Shards = make(map[int][]byte)
	for _, id := range args.Shards {
		shard := kv.shardDB[id]
		reply.Shards[id] = shard.Dump()
	}

	reply.ConfNum = kv.currConfig.Num
	reply.Err = common.OK

	return
}

func (kv *ShardKV) EraseShard(args *common.EraseShardArgs, reply *common.EraseShardReply) (e error) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = common.ErrWrongLeader
		return
	}

	kv.log.Infof("KVServer %d receive EraseShard: %v", kv.me, *args)

	kv.mu.RLock()
	if kv.currConfig.Num > args.ConfNum {
		reply.ConfNum = kv.currConfig.Num
		reply.Err = common.OK
		kv.mu.RUnlock()
		return
	}
	if kv.currConfig.Num != args.ConfNum {
		reply.Err = common.ErrDiffConf
		reply.ConfNum = kv.currConfig.Num
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	cmd := EraseShardCmd{
		CmdBase: CmdBase{},
		Shards:  args.Shards,
		ConfNum: kv.currConfig.Num,
	}
	res, err := kv.raftStartCmdWait(common.CmdTypeEraseShard, utils.MsgpEncode(&cmd))
	if err != common.OK {
		reply.Err = err
		reply.ConfNum = kv.currConfig.Num
		return
	}
	reply.Err = res.GetErr()
	reply.ConfNum = kv.currConfig.Num

	return
}

func (kv *ShardKV) TransferLeader(args *raft.TransferLeaderArgs, reply *raft.TransferLeaderReply) (e error) {
	kv.mu.RLock()
	conf := kv.currConfig
	kv.mu.RUnlock()

	ngs := conf.Groups[kv.gid]
	for _, ng := range ngs {
		if ng.NodeId == args.NodeId {
			args.Peer = ng.RaftPeer
			return kv.rf.TransferLeader(args, reply)
		}
	}
	reply.Err = common.ErrFailed
	return
}

func (kv *ShardKV) checkShardInCharge(key string) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	shardId := common.Key2shard(key)
	if kv.currConfig.Shards[shardId] != kv.gid {
		return false
	}
	if shard, ok := kv.shardDB[shardId]; !ok || !(shard.Status == common.SERVING || shard.Status == common.WAITING) {
		return false
	}
	return true
}

func (kv *ShardKV) raftStartCmdNoWait(cmdType uint8, cmdBody []byte) common.Err {
	wrap := utils.EncodeCmdWrap(cmdType, cmdBody)

	var idx, term int
	var isLeader bool

	if idx, term, isLeader = kv.rf.Start(wrap); !isLeader {
		return common.ErrWrongLeader
	}
	kv.log.Debugf("KVServer %d call raft.start res: Idx=%d term=%d isLeader=%v", kv.me, idx, term, isLeader)

	return common.OK
}

func (kv *ShardKV) raftStartCmdWait(cmdType uint8, cmdBody []byte) (ApplyRes, common.Err) {
	cmdWrap := utils.EncodeCmdWrap(cmdType, cmdBody)

	var idx, term int
	var isLeader bool

	if idx, term, isLeader = kv.rf.Start(cmdWrap); !isLeader {
		return nil, common.ErrWrongLeader
	}
	kv.log.Debugf("Group %d KVServer %d call raft.start res: Idx=%d term=%d isLeader=%v", kv.gid, kv.me, idx, term, isLeader)

	kv.mu.Lock()
	waitC := kv.getWaitCh(idx)
	kv.mu.Unlock()
	defer kv.delWaitChLock(idx)

	kv.log.Debugf("KVServer %d waiting at the channel %v, idx=%d", kv.me, waitC, idx)
	// wait for being applied
	select {
	case res := <-waitC:
		kv.log.Debugf("KVServer %d receive res from waiting channel %v", kv.me, res.GetIdx())
		return res, common.OK

	case <-time.After(time.Second * 1):
		kv.log.Warnf("KVServer %d op %v at Idx %d has not commited after 1 secs", kv.me, idx)
		return nil, common.ErrFailed
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
	for kv.lastApplied < target && !kv.Killed(){
		kv.appliedCond.Wait()
	}
	kv.appliedCond.L.Unlock()
}

func (kv *ShardKV) isDuplicated(key string, cid, seq int64) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	shardId := common.Key2shard(key)
	shard := kv.shardDB[shardId]
	return shard.IfDuplicateAndSet(cid, seq, false)
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.appliedCond.Broadcast()
	for i := 0; i < kv.numDaemon; i++ {
		kv.KilledC <- i
	}
	for i := 0; i < kv.numDaemon; i++ {
		name := <-kv.exitedC
		kv.log.Warnf("KVServer %d daemon function %s exited", kv.me, name)
	}
	kv.rf.Kill()
	kv.store.Close()
	kv.log.Warnf("KVServer %d exited", kv.me)

}

func (kv *ShardKV) Killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func StartServer(raftDataDir, logFileName string, logFileCap uint64 ,me int, peers int, gid int,
	store Store, rpcFunc netw.RpcFunc, logLevel string) *ShardKV {
	var err error

	kv := new(ShardKV)
	
	kv.log, _ = common.InitLogger(logLevel, fmt.Sprintf("Replica-%d-%d", gid, me))

	kv.rpcFunc = rpcFunc

	kv.me = me
	kv.gid = gid

	kv.store = store

	kv.applyCh = make(chan raft.ApplyMsg)

	snapshotPath := fmt.Sprintf("%s/snapshot-%d-%d", raftDataDir, gid, me)
	raftstatePath := fmt.Sprintf("%s/raftstate-%d-%d", raftDataDir, gid, me)

	if kv.persister, err = raft.MakeDiskPersister(snapshotPath, raftstatePath); err != nil {
		kv.log.Fatalf("failed to make disk persister: %v", err)
	}

	kv.rf = raft.Make(kv.rpcFuncImpl, peers, me, kv.persister, kv.applyCh, true, logFileName, logFileCap, logLevel)

	kv.opApplied = make(map[int]chan ApplyRes)
	kv.appliedCond = sync.NewCond(&kv.mu)

	kv.numDaemon = 4

	kv.KilledC = make(chan int, kv.numDaemon + 1)
	kv.exitedC = make(chan string, kv.numDaemon)

	kv.shardDB = make(map[int]*Shard)

	kv.currConfig = common.ConfigV1{
		Num: 0,
		Shards: [common.NShards]int{},
		Groups: map[int][]common.ConfigNodeGroup{},
	}

	if err := kv.RecoverFromStore(); err != nil {
		kv.log.Errorln(err)
	}

	go kv.checkpointer()
	go kv.applyer()
	// go kv.confUpdater()
	go kv.shardPuller()
	go kv.shardEraser()

	return kv
}