package replica

import (
	"fmt"
	"log"
	"sync"
	"time"

	"mrkv/src/master"
)

const (
	PullerTick = 70*time.Millisecond
	EraserTick = 70*time.Millisecond
	ReConfTick = 1000*time.Millisecond
)


func (kv *ShardKV) canPullConfig() bool {
	for _, shard := range kv.shardDB {
		if !(shard.Status == SERVING || shard.Status == INVALID) {
			return false
		}
	}
	return true
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

			var latestConf master.Config
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

			shards := kv.getShards(PULLING)
			if len(shards) == 0 {
				kv.mu.RUnlock()
				continue
			}

			shardsByExOwner := make(map[int][]int)
			for _, shard := range shards {
				if shard.Status != PULLING {
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
						},
						Shards: shardsToPull,
					}
					reply := PullShardReply{}

					ends, ok := kv.ends[groupId]
					if !ok {
						log.Printf("no ends found by gid %d", groupId)
						return
					}
					for i := 0; i < len(ends); i++ {
						if ok := ends[i].Call(fmt.Sprintf("Replica-%d-%d.PullShard", groupId, i), &args, &reply); ok && reply.Err == OK {
							kv.log.Infof("KVServer %d ShardPuller: send PullShard to group %d success, raft start InstallShardCmd ", kv.me, groupId)
							err := kv.raftStartCmdNoWait(InstallShardCmd {
								CmdBase: &CmdBase {
									Type: CmdInstallShard,
								},
								Shards: reply.Shards,
								ConfNum: confNum,
							})
							if err == ErrWrongLeader {
								kv.log.Debugf("KVServer %d ShardPuller: want to send InstallShardCmd to peer, but im not leader!!",
									kv.me)
							}
							break
						} else if reply.Err == ErrDiffConf {
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

			shards := kv.getShards(WAITING)
			if len(shards) == 0 {
				kv.mu.RUnlock()
				continue
			}

			shardsByExOwner := make(map[int][]int)
			for _, shard := range shards {
				if shard.Status != WAITING {
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
						},
						Shards: shardsToErase,
					}
					reply := EraseShardReply{}

					ends, ok := kv.ends[groupId]
					if !ok {
						log.Printf("no ends found by gid %d", groupId)
						return
					}

					for i, end := range ends {
						if ok := end.Call(fmt.Sprintf("Replica-%d-%d.EraseShard", groupId, i), &args, &reply); ok && reply.Err == OK {
							kv.log.Infof("KVServer %d ShardEraser: send EraseShard to group %d success, raft start StopWaitingShardCmd ", kv.me, groupId)
							err := kv.raftStartCmdNoWait(StopWaitingShardCmd {
								CmdBase: &CmdBase {
									Type: CmdStopWaiting,
								},
								Shards:  shardsToErase,
								ConfNum: confNum,
							})
							if err == ErrWrongLeader {
								kv.log.Debugf("KVServer %d ShardEraser: want to send StopWaitingShardCmd to peer, but im not leader!!",
									kv.me)
							}
							break
						} else if reply.Err == ErrDiffConf {
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


func (kv *ShardKV) getShards(status ShardStatus) []*Shard {
	shards := make([]*Shard, 0)
	for _, shard := range kv.shardDB {
		if shard == nil || shard.Status != status {
			continue
		}
		shards = append(shards, shard)
	}

	return shards
}

func (kv *ShardKV) executeGet(key string) (string, Err) {
	shardIdx := key2shard(key)
	shard := kv.shardDB[shardIdx]
	if kv.currConfig.Shards[shardIdx] != kv.gid || !(shard.Status == SERVING || shard.Status == WAITING) {
		return "", ErrWrongGroup
	}
	fullKey := fmt.Sprintf(ShardUserDataPattern, shard.Idx, key)
	val, err := shard.Store.Get(fullKey)
	if err != nil  {
		return "", ErrFailed
	} else if val == nil {
		return "",ErrNoKey
	} else {
		return string(val), OK
	}
}