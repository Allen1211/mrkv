package replica

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"mrkv/src/common"
	"mrkv/src/master"
	"mrkv/src/netw"
)

const (
	PullerTick = 70*time.Millisecond
	EraserTick = 70*time.Millisecond
	ReConfTick = 1000*time.Millisecond
)


func (kv *ShardKV) canPullConfig() bool {
	for _, shard := range kv.shardDB {
		if !(shard.Status == master.SERVING || shard.Status == master.INVALID) {
			return false
		}
	}
	return true
}

func (kv *ShardKV) InitConfig(prevConfig, currConfig master.ConfigV1)  {
	kv.log.Infof("init config, Num=%d; prevConfig: %v, currConfig %v", currConfig.Num, prevConfig, currConfig)

	kv.SetPrevConfig(prevConfig)
	kv.SetCurrConfig(currConfig)

	// shards we gain
	for shardId, gid := range currConfig.Shards {
		if gid != kv.gid {
			continue
		}
		if prevConfig.Shards[shardId] == kv.gid {
			// kv.log.Fatalf("InitConfig: shard %d is already owned but this group is newly join", shardId)
			kv.log.Warnf("InitConfig: shard %d is already owned but this group is newly join", shardId)
		}
		if prevConfig.Num == 0 || prevConfig.Shards[shardId] == 0 {
			kv.shardDB[shardId] = MakeShard(shardId, master.SERVING, 0, kv.store)
		} else {
			var shard *Shard
			var ok bool
			if shard, ok = kv.shardDB[shardId]; !ok {
				shard = MakeShard(shardId, master.INVALID, 0, kv.store)
				kv.shardDB[shardId] = shard
			}
			shard.SetExOwner(prevConfig.Shards[shardId])
			shard.SetStatus(master.PULLING)
			kv.log.Infof("KVServer %d gain shard %d, now status is %v, exOwner is %d",
				kv.me, shardId, shard.Status, shard.ExOwner)
		}
	}

	// shards we lost
	for shardId, gid := range currConfig.Shards {
		if gid == kv.gid || prevConfig.Shards[shardId] != kv.gid {
			continue
		}
		log.Warnf("InitConfig: shard %d is owned in prevConfig but this group is newly join", shardId)
	}

	kv.log.Debugf("KVServer %d finish init config, now current config: ", kv.me)
	kv.printLatestConfig(kv.currConfig)
}

func (kv *ShardKV) UpdateConfig(config master.ConfigV1)  {
	kv.mu.RLock()
	if !kv.canPullConfig() {
		kv.mu.RUnlock()
		return
	}
	currConf := kv.currConfig
	kv.mu.RUnlock()

	if config.Num == currConf.Num + 1 {
		kv.log.Infof("KVServer %d pull latest currConfig, Num=%d", kv.me, config.Num)
		cmd := ConfCmd {
			CmdBase: &CmdBase{
				Type: CmdConf,
			},
			Config: config,
		}
		kv.raftStartCmdNoWait(cmd)
	}
}

func (kv *ShardKV) confUpdater() {
	tick := time.Tick(ReConfTick)
	for {
		select {
		case <-kv.KilledC:
			kv.log.Debugf("KVServer %d has been killed, stop confUpdater loop", kv.me)
			kv.exitedC <- runFuncName()
			return
		case <-tick:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}
			kv.mu.RLock()
			if ok := kv.canPullConfig(); !ok {
				kv.mu.RUnlock()
				continue
			}
			currConf := kv.currConfig
			kv.mu.RUnlock()

			var latestConf master.ConfigV1
			f := func() {
				latestConf = kv.mck.Query(currConf.Num + 1)
			}
			if !willTimeout(f, 1*time.Second) {
				if latestConf.Num == currConf.Num + 1 {
					kv.log.Infof("KVServer %d pull latest currConfig, Num=%d", kv.me, latestConf.Num)
					cmd := ConfCmd {
						CmdBase: &CmdBase{
							Type: CmdConf,
						},
						Config: latestConf,
					}
					kv.raftStartCmdNoWait(cmd)
				}
			}
		}
	}
}


func (kv *ShardKV) shardPuller() {
	tick := time.Tick(PullerTick)
	for {
		select {
		case <-kv.KilledC:
			kv.log.Debugf("KVServer %d has been killed, stop shardPuller loop", kv.me)
			kv.exitedC <- runFuncName()
			return
		case <-tick:

			if _, isLeader := kv.rf.GetState(); !isLeader {
				continue
			}

			kv.mu.RLock()

			shards := kv.getShards(master.PULLING)
			if len(shards) == 0 {
				kv.mu.RUnlock()
				continue
			}

			shardsByExOwner := make(map[int][]int)
			for _, shard := range shards {
				if shard.Status != master.PULLING {
					continue
				}
				if _, ok := shardsByExOwner[shard.ExOwner]; !ok {
					shardsByExOwner[shard.ExOwner] = make([]int, 0)
				}
				shardsByExOwner[shard.ExOwner] = append(shardsByExOwner[shard.ExOwner], shard.Idx)
			}

			kv.log.Infof("KVServer %d ShardPuller: find shards that need to pull: %v",
				kv.me, shardsByExOwner)
			kv.log.Infof("KVServer %d ShardPuller: current config: ", kv.me)
			kv.printLatestConfig(kv.currConfig)

			var wg sync.WaitGroup
			wg.Add(len(shardsByExOwner))
			for gid, shards := range shardsByExOwner {
				go func(shardsToPull []int, confNum int, groupId int) {
					defer wg.Done()
					args := PullShardArgs {
						BaseArgs: BaseArgs{
							ConfNum: confNum,
							Gid: groupId,
						},
						Shards: shardsToPull,
					}
					reply := PullShardReply{}

					nodesOfGroup, ok := kv.prevConfig.Groups[groupId]
					if !ok {
						kv.log.Errorf("no node found by gid %d", groupId)
						return
					}
					for _, node := range nodesOfGroup {
						if ok := kv.rpcFunc(netw.ApiPullShard, &args, &reply, node.NodeId, groupId); ok && reply.Err == common.OK {
							kv.log.Infof("KVServer %d ShardPuller: send PullShard to group %d success, raft start InstallShardCmd ", kv.me, groupId)
							err := kv.raftStartCmdNoWait(InstallShardCmd {
								CmdBase: &CmdBase {
									Type: CmdInstallShard,
								},
								Shards: reply.Shards,
								ConfNum: confNum,
							})
							if err == common.ErrWrongLeader {
								kv.log.Debugf("KVServer %d ShardPuller: want to send InstallShardCmd to peer, but im not leader!!",
									kv.me)
							}
							break
						} else {
							kv.log.Warnf("KVServer %d ShardPuller: send PullShard to group %d failed, err: %v, our %d != %d",
								kv.me, groupId, reply.Err, confNum, reply.ConfNum)
						}
					}

				}(shards, kv.currConfig.Num, gid)
			}

			kv.mu.RUnlock()
			kv.log.Debugf("KVServer %d ShardPuller: start waiting..", kv.me)
			wg.Wait()
			kv.log.Infof("KVServer %d ShardPuller: finish waiting..", kv.me)
		}
	}
}

func (kv *ShardKV) shardEraser() {

	tick := time.Tick(EraserTick)
	for {
		select {
		case <-kv.KilledC:
			kv.log.Debugf("KVServer %d has been killed, stop shardEraser loop", kv.me)
			kv.exitedC <- runFuncName()
			return
		case <-tick:

			if _, isLeader := kv.rf.GetState(); !isLeader {
				// kv.mu.RUnlock()
				continue
			}
			kv.mu.RLock()

			shards := kv.getShards(master.WAITING)
			if len(shards) == 0 {
				kv.mu.RUnlock()
				continue
			}

			shardsByExOwner := make(map[int][]int)
			for _, shard := range shards {
				if shard.Status != master.WAITING {
					continue
				}
				if _, ok := shardsByExOwner[shard.ExOwner]; !ok {
					shardsByExOwner[shard.ExOwner] = make([]int, 0)
				}
				shardsByExOwner[shard.ExOwner] = append(shardsByExOwner[shard.ExOwner], shard.Idx)
			}

			kv.log.Infof("KVServer %d ShardEraser: find shards that need to erase: %v",
				kv.me, shardsByExOwner)
			kv.log.Infof("KVServer %d ShardEraser: current config: ", kv.me)
			kv.printLatestConfig(kv.currConfig)

			var wg sync.WaitGroup
			wg.Add(len(shardsByExOwner))
			for gid, shards := range shardsByExOwner {
				go func(shardsToErase []int, confNum int, groupId int) {
					defer wg.Done()
					args := EraseShardArgs {
						BaseArgs: BaseArgs{
							ConfNum: confNum,
							Gid: groupId,
						},
						Shards: shardsToErase,
					}
					reply := EraseShardReply{}

					nodesOfGroup, ok := kv.prevConfig.Groups[groupId]
					if !ok {
						kv.log.Errorf("no node found by gid %d", groupId)
						return
					}
					for _, node := range nodesOfGroup {
						if ok := kv.rpcFunc(netw.ApiEraseShard, &args, &reply, node.NodeId); ok && reply.Err == common.OK {
							kv.log.Infof("KVServer %d ShardEraser: send EraseShard to group %d success, raft start StopWaitingShardCmd ", kv.me, groupId)
							err := kv.raftStartCmdNoWait(StopWaitingShardCmd {
								CmdBase: &CmdBase {
									Type: CmdStopWaiting,
								},
								Shards:  shardsToErase,
								ConfNum: confNum,
							})
							if err == common.ErrWrongLeader {
								kv.log.Debugf("KVServer %d ShardEraser: want to send StopWaitingShardCmd to peer, but im not leader!!",
									kv.me)
							}
							break
						} else {
							kv.log.Warnf("KVServer %d ShardEraser: send EraseShard to group %d failed, err: %v, our %d != %d",
								kv.me, groupId, reply.Err, confNum, reply.ConfNum)
						}
					}

				}(shards, kv.currConfig.Num, gid)
			}

			kv.mu.RUnlock()
			wg.Wait()
			kv.log.Infof("KVServer %d ShardEraser: finish waiting..", kv.me)
		}
	}

}


func (kv *ShardKV) getShards(status master.ShardStatus) []*Shard {
	shards := make([]*Shard, 0)
	for _, shard := range kv.shardDB {
		if shard == nil || shard.Status != status {
			continue
		}
		shards = append(shards, shard)
	}

	return shards
}

func (kv *ShardKV) executeGet(key string) ([]byte, common.Err) {
	shardIdx := master.Key2shard(key)
	shard := kv.shardDB[shardIdx]
	if kv.currConfig.Shards[shardIdx] != kv.gid || !(shard.Status == master.SERVING || shard.Status == master.WAITING) {
		return nil, common.ErrWrongGroup
	}
	fullKey := fmt.Sprintf(ShardUserDataPattern, shard.Idx, key)
	val, err := shard.Store.Get(fullKey)
	if err != nil  {
		return nil, common.ErrFailed
	} else if val == nil {
		return nil, common.ErrNoKey
	} else {
		return val, common.OK
	}
}