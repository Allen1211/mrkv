package replica

import (
	"fmt"

	"mrkv/src/common"
	"mrkv/src/master"
	"mrkv/src/raft"
)

func (kv *ShardKV) applyer() {
	for {
		select {
		case <-kv.KilledC:
			kv.log.Debugf("KVServer %d has been killed, stop applyer loop", kv.me)
			kv.exitedC <- runFuncName()
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				continue
			}
			if snapMsg, ok := msg.Command.(raft.InstallSnapshotMsg); ok {
				msg.Command = SnapshotCmd {
					CmdBase: &CmdBase{
						Type: CmdSnap,
					},
					SnapInfo: snapMsg,
				}
			} else if _, ok := msg.Command.(raft.EmptyCmd); ok {
				msg.Command = EmptyCmd{
					CmdBase: &CmdBase {
						Type: CmdEmpty,
					},
				}
			} else {
				kv.mu.RLock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.RUnlock()
					continue
				}
				kv.mu.RUnlock()
			}
			cmdI := msg.Command.(Cmd)
			if cmdI.GetType() != CmdSnap {
				// fmt.Printf("@@@Group %d KVServer %d apply %d type %v ConfNum %d cmd: %v",
				// 	kv.gid, kv.me, msg.CommandIndex, cmdI.GetType(), kv.currConfig.Num, cmdI)
			}
			switch cmdI.GetType() {
			case CmdEmpty:
				// cmd := cmdI.(EmptyCmd)
				kv.log.Debugf("KVServer %d Applyer received empty msg from applyCh: %d, term is %d",
					kv.me, msg.CommandIndex, msg.CommandTerm)

			case CmdKV:
				cmd := cmdI.(KVCmd)
				kv.log.Infof("KVServer %d Applyer received kv msg from applyCh: %d", kv.me, msg.CommandIndex)

				var val []byte
				var ok bool
				var err common.Err
				val, ok, err, _ = kv.applyKVCmd(cmd)
				if err == common.ErrDuplicate {
					kv.log.Infof("KVServer %d Applyer received kv msg from applyCh: idx=%d is duplicated", kv.me, msg.CommandIndex)
					// continue
				}
				res := &KVCmdApplyRes{
					ApplyResBase: &ApplyResBase{
						cmdType: CmdKV,
						idx:     msg.CommandIndex,
						err:     err,
					},
					op:  cmd.Op,
					val: val,
					ok:  ok,
				}
				kv.sendResToWaitC(res, msg.CommandTerm)

			case CmdSnap:
				kv.log.Infof("KVServer %d Applyer received install snapshot msg from applyCh", kv.me)

				cmd := cmdI.(SnapshotCmd)
				kv.applySnapshotCmd(cmd)

			case CmdConf:
				kv.log.Infof("KVServer %d Applyer received re-configure msg from applyCh idx=%d", kv.me, msg.CommandIndex)

				cmd := cmdI.(ConfCmd)
				kv.applyReConfigure(cmd)

			case CmdInstallShard:
				kv.log.Infof("KVServer %d Applyer received install shard msg from applyCh idx=%d", kv.me, msg.CommandIndex)

				cmd := cmdI.(InstallShardCmd)
				kv.applyInstallShard(cmd)

			case CmdEraseShard:
				kv.log.Infof("KVServer %d Applyer received erase shard msg from applyCh, idx=%d", kv.me, msg.CommandIndex)

				cmd := cmdI.(EraseShardCmd)
				kv.applyEraseShard(cmd)

				res := &EraseShardCmdApplyRes{
					ApplyResBase: &ApplyResBase{
						cmdType: CmdEraseShard,
						idx:     msg.CommandIndex,
						err:     common.OK,
					},
				}
				kv.sendResToWaitC(res, msg.CommandTerm)

			case CmdStopWaiting:
				kv.log.Infof("KVServer %d Applyer received stop waiting msg from applyCh, idx=%d", kv.me, msg.CommandIndex)
				cmd := cmdI.(StopWaitingShardCmd)
				kv.applyStopWaitingShardCmd(cmd)
			default:
				fmt.Println(cmdI)
				panic("unreconized cmd type")
			}

			if cmdI.GetType() != CmdSnap {
				kv.mu.Lock()
				kv.SetLastApplied(msg.CommandIndex)
				kv.appliedCond.Broadcast()
				kv.mu.Unlock()
			}
			kv.log.Infof("KVServer %d Applyer finished apply msg idx=%d", kv.me, msg.CommandIndex)

		}
	}
}

func (kv *ShardKV) applyKVCmd(cmd KVCmd) ([]byte, bool, common.Err, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := cmd.Op
	key, newVal := op.Key, op.Value

	shardIdx := master.Key2shard(key)
	shard := kv.shardDB[shardIdx]
	if kv.currConfig.Shards[shardIdx] != kv.gid || !(shard.Status == master.SERVING || shard.Status == master.WAITING) {
		return nil, false, common.ErrWrongGroup, false
	}

	if op.Type == OpPut || op.Type == OpAppend {
		if shard.IfDuplicateAndSet(cmd.Cid, cmd.Seq, true) {
			return	nil, false, common.ErrDuplicate, false
		}
		kv.log.Debugf("KVServer %d update CkMaxSeq to %d, cmd=%v", kv.me, cmd.Seq, *cmd.CmdBase)
	}

	db := shard.Store
	fullKey := fmt.Sprintf(ShardUserDataPattern, shard.Idx, key)
	switch op.Type {
	case OpGet:
		val, err := db.Get(fullKey)
		if err != nil {
			return nil, false, common.ErrFailed, true
		}
		if val == nil {
			return nil, false, common.ErrNoKey, true
		} else {
			return val, true, common.OK, true
		}
	case OpPut:
		if err := db.Put(fullKey, newVal); err != nil {
			return nil, false, common.ErrFailed, true
		} else {
			return newVal, true, common.OK, true
		}
	case OpAppend:
		if err := db.Append(fullKey, newVal); err != nil {
			return nil, false, common.ErrFailed, true
		} else {
			return newVal, true, common.OK, true
		}
	case OpDelete:
		if err := db.Delete(fullKey); err != nil {
			return nil, false, common.ErrFailed, true
		} else {
			return newVal, true, common.OK, true
		}
	default:
		panic("unreconized op type")
	}
}

func (kv *ShardKV) applySnapshotCmd(cmd SnapshotCmd) {
	snapshotInfo := cmd.SnapInfo

	kv.log.Infof("KVServer %d received snapshot cmd: lastIncludedIdx=%d", snapshotInfo.LastIncludedIdx)
	kv.mu.Lock()

	if !kv.rf.CondInstallSnapshot(snapshotInfo.LastIncludedTerm, snapshotInfo.LastIncludedIdx, snapshotInfo.LastIncludedEndLSN, snapshotInfo.Data) {
		kv.mu.Unlock()
		return
	}
	kv.applySnapshot(snapshotInfo.Data)
	kv.SetLastApplied(snapshotInfo.LastIncludedIdx)
	kv.log.Infof("KVServer %d apply snapshot, lastApplied update to %d", kv.me, kv.lastApplied)
	kv.mu.Unlock()
}

func (kv *ShardKV) applyReConfigure(cmd ConfCmd) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	newConfig := cmd.Config
	currConfig := kv.currConfig

	kv.log.Infof("KVServer %d applying re-configure cmd: %v, newConfig Num=%d, currConfig Num=%d",
		kv.me, cmd, newConfig.Num, currConfig.Num)

	if currConfig.Num + 1 != newConfig.Num {
		kv.log.Infof("KVServer %d cannot apply re-configure cmd: %v, newConfig Num %d != currConfig Num %d + 1",
			kv.me, cmd, newConfig.Num, currConfig.Num)
		return true
	}

	// shards we gain
	for shardId, gid := range newConfig.Shards {
		if gid != kv.gid || currConfig.Shards[shardId] == kv.gid {
			continue
		}
		if currConfig.Num == 0 || currConfig.Shards[shardId] == 0 {
			// first currConfig, install shard but dont need to pull from other cluster
			kv.shardDB[shardId] = MakeShard(shardId, master.SERVING, 0, kv.store)
		} else {

			var shard *Shard
			var ok bool

			if shard, ok = kv.shardDB[shardId]; !ok {
				shard = MakeShard(shardId, master.INVALID, 0, kv.store)
				kv.shardDB[shardId] = shard
			}
			shard.SetExOwner(currConfig.Shards[shardId])
			shard.SetStatus(master.PULLING)

			kv.log.Infof("KVServer %d gain shard %d, now status is %v, exOwner is %d",
				kv.me, shardId, shard.Status, shard.ExOwner)
		}
	}

	// shards we lost
	for shardId, gid := range newConfig.Shards {
		if gid == kv.gid || currConfig.Shards[shardId] != kv.gid {
			continue
		}

		if currConfig.Num == 0 {
			// first currConfig, do nothing
			kv.shardDB[shardId] = MakeShard(shardId, master.SERVING, 0, kv.store)
		} else {

			var shard *Shard
			var ok bool

			if shard, ok = kv.shardDB[shardId]; !ok {
				shard = MakeShard(shardId, master.SERVING, kv.gid, kv.store)
				kv.shardDB[shardId] = shard
			}
			shard.SetExOwner(kv.gid)

			if gid == 0 {
				shard.SetStatus(master.INVALID)
			} else {
				switch shard.Status {
				case master.INVALID:
				case master.SERVING:
					// we lost a shard
					shard.SetStatus(master.ERASING)
				case master.ERASING:
				case master.PULLING:
					shard.SetStatus(master.ERASING)
				}
			}

			kv.log.Infof("KVServer %d lost shard %d, now status is %v, exOwner is %d, nowOwner is %d",
				kv.me, shardId, shard.Status, shard.ExOwner, newConfig.Shards[shardId])
		}
	}

	kv.SetPrevConfig(currConfig)
	kv.SetCurrConfig(newConfig)

	kv.log.Debugf("KVServer %d finish apply re-configure cmd, now current config: ", kv.me)
	kv.printLatestConfig(kv.currConfig)

	return true
}

func (kv *ShardKV) applyInstallShard(cmd InstallShardCmd) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if cmd.ConfNum != kv.currConfig.Num {
		kv.log.Infof("KVServer %d cannot apply install shard cmd, because Config Num not match %d != %d",
			kv.me, kv.currConfig.Num, cmd.ConfNum)
		return true
	}
	for shardId, shard := range cmd.Shards {
		if kv.currConfig.Shards[shardId] != kv.gid {
			continue
		}
		if localShard := kv.shardDB[shardId]; localShard.Status == master.PULLING {

			localShard.Install(shard)
			localShard.SetExOwner( kv.prevConfig.Shards[shardId])
			localShard.SetStatus(master.WAITING)
			localShard.SetVersion(kv.currConfig.Num)

			kv.log.Infof("KVServer %d shard %d installed, PULLING -> WAITING, exOwner is %d",
				kv.me, shardId, localShard.ExOwner)

		} else if localShard.Status != master.PULLING {
			kv.log.Infof("KVServer %d shard %d pull shard cmd apply duplicated, break", kv.me, shardId)
			continue
		}
	}
	return true
}

func (kv *ShardKV) applyEraseShard(cmd EraseShardCmd) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.currConfig.Num != cmd.ConfNum {
		kv.log.Infof("KVServer %d cannot apply erase shard cmd: %v, because Config Num not match %d != %d",
			kv.me, cmd, kv.currConfig.Num, cmd.ConfNum)
		return true
	}

	for _, shardId := range cmd.Shards {
		if shard, ok := kv.shardDB[shardId]; ok && (shard.Status == master.ERASING || shard.Status == master.INVALID){
			kv.log.Infof("KVServer %d shard %d erased, ERASING -> INVALID", kv.me, shardId)
			shard.SetStatus(master.INVALID)
			if kv.currConfig.Num > shard.GetVersion() {
				shard.ClearUserData()
				kv.log.Infof("KVServer %d do clear shard data", kv.me)
			} else {
				kv.log.Infof("KVServer %d dont need to clear shard data", kv.me)
			}
		} else if shard.Status != master.ERASING {
			kv.log.Infof("KVServer %d shard %d erase shard cmd apply duplicated, break", kv.me, shardId)
			// break
			continue
		}
	}
	return true
}

func (kv *ShardKV) applyStopWaitingShardCmd(cmd StopWaitingShardCmd) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.currConfig.Num != cmd.ConfNum {
		kv.log.Infof("KVServer %d cannot apply stop waiting cmd: %v, because Config Num not match %d != %d",
			kv.me, cmd, kv.currConfig.Num, cmd.ConfNum)
		return true
	}

	kv.log.Infof("KVServer %d apply stop waiting cmd: %v", kv.me, cmd)
	for _, shardId := range cmd.Shards {
		if shard, ok := kv.shardDB[shardId]; ok && shard.Status == master.WAITING {
			kv.log.Infof("KVServer %d shard %d stop waiting, WAITING -> SERVING", kv.me, shardId)
			shard.SetStatus(master.SERVING)
		} else if shard.Status != master.WAITING {
			kv.log.Infof("KVServer %d shard %d stop waiting cmd apply duplicated, break", kv.me, shardId)
			continue
		}
	}

	return true
}

func (kv *ShardKV) sendResToWaitC(res ApplyRes, msgTerm int) {
	if term, isLeader := kv.rf.GetState(); !isLeader || term != msgTerm {
		kv.log.Debugf("KVServer %d Applyer abort send apply res %d to wait channel, because isLeader:%v, currTerm=%d msgTerm=%d",
			kv.me, res.GetIdx(), isLeader, term, msgTerm)
		return
	}

	var idx = res.GetIdx()

	kv.mu.Lock()
	c := kv.getWaitCh(idx)
	kv.mu.Unlock()

	// notify all waiting client request
	select {
	case c <- res:
		kv.log.Debugf("KVServer %d Applyer successfully send apply result %v to waiting channel %v", kv.me, res.GetIdx(), c)
	default:
	}
}
