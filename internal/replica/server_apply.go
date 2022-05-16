package replica

import (
	"encoding/binary"
	"fmt"

	"github.com/allen1211/mrkv/pkg/common"
	"github.com/allen1211/mrkv/pkg/common/utils"
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
			wrap := utils.DecodeCmdWrap(msg.Command)
			needUpdateLastApplied := true
			if wrap.Type != common.CmdTypeSnap {
				kv.mu.RLock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.RUnlock()
					continue
				}
				kv.mu.RUnlock()
			} else {
				needUpdateLastApplied = false
			}

			switch wrap.Type {
			case common.CmdTypeEmpty:
				kv.log.Debugf("KVServer %d Applyer received empty msg from applyCh: %d, term is %d",
					kv.me, msg.CommandIndex, msg.CommandTerm)

			case common.CmdTypeKV:
				cmd := KVCmd{}
				utils.MsgpDecode(wrap.Body,  &cmd)
				kv.log.Infof("KVServer %d Applyer received kv msg from applyCh: %d", kv.me, msg.CommandIndex)

				var val []byte
				var ok bool
				var err common.Err

				if cmd.Op.Type == common.OpGet {
					val, ok, err, _ = kv.applyKVCmd(cmd)
				} else {
					val, ok, err, _ = kv.applyKVCmdBatch(cmd, msg.CommandIndex)
					needUpdateLastApplied = false
				}

				if err == common.ErrDuplicate {
					kv.log.Infof("KVServer %d Applyer received kv msg from applyCh: idx=%d is duplicated", kv.me, msg.CommandIndex)
					// continue
				}
				res := &KVCmdApplyRes{
					ApplyResBase: &ApplyResBase{
						cmdType: common.CmdKV,
						idx:     msg.CommandIndex,
						err:     err,
					},
					op:  cmd.Op,
					val: val,
					ok:  ok,
				}
				kv.sendResToWaitC(res, msg.CommandTerm)

			case common.CmdTypeSnap:
				kv.log.Infof("KVServer %d Applyer received install snapshot msg from applyCh", kv.me)

				cmd := SnapshotCmd{}
				utils.MsgpDecode(wrap.Body,  &cmd.SnapInfo)
				kv.applySnapshotCmd(cmd)

			case common.CmdTypeConf:
				kv.log.Infof("KVServer %d Applyer received re-configure msg from applyCh idx=%d", kv.me, msg.CommandIndex)

				cmd := ConfCmd{}
				utils.MsgpDecode(wrap.Body,  &cmd)
				kv.applyReConfigure(cmd)

			case common.CmdTypeInstallShard:
				kv.log.Infof("KVServer %d Applyer received install shard msg from applyCh idx=%d", kv.me, msg.CommandIndex)

				cmd := InstallShardCmd{}
				utils.MsgpDecode(wrap.Body,  &cmd)
				kv.applyInstallShard(cmd)

			case common.CmdTypeEraseShard:
				kv.log.Infof("KVServer %d Applyer received erase shard msg from applyCh, idx=%d", kv.me, msg.CommandIndex)

				cmd := EraseShardCmd{}
				utils.MsgpDecode(wrap.Body,  &cmd)

				kv.applyEraseShard(cmd)

				res := &EraseShardCmdApplyRes{
					ApplyResBase: &ApplyResBase{
						cmdType: common.CmdEraseShard,
						idx:     msg.CommandIndex,
						err:     common.OK,
					},
				}
				kv.sendResToWaitC(res, msg.CommandTerm)

			case common.CmdTypeStopWaiting:
				kv.log.Infof("KVServer %d Applyer received stop waiting msg from applyCh, idx=%d", kv.me, msg.CommandIndex)
				cmd := StopWaitingShardCmd{}
				utils.MsgpDecode(wrap.Body,  &cmd)

				kv.applyStopWaitingShardCmd(cmd)
			default:
				panic("unreconized cmd type")
			}

			if needUpdateLastApplied {
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

	shardIdx := common.Key2shard(key)
	shard := kv.shardDB[shardIdx]
	if kv.currConfig.Shards[shardIdx] != kv.gid || !(shard.Status == common.SERVING || shard.Status == common.WAITING) {
		return nil, false, common.ErrWrongGroup, false
	}

	if op.Type == common.OpPut || op.Type == common.OpAppend || op.Type == common.OpDelete {
		if shard.IfDuplicateAndSet(cmd.Cid, cmd.Seq, true) {
			return	nil, false, common.ErrDuplicate, false
		}
		kv.log.Debugf("KVServer %d update CkMaxSeq to %d, cmd=%v", kv.me, cmd.Seq, cmd.CmdBase)
	}

	db := shard.Store
	fullKey := fmt.Sprintf(ShardUserDataPattern, shard.Idx, key)
	switch op.Type {
	case common.OpGet:
		val, err := db.Get(fullKey)
		if err != nil {
			return nil, false, common.ErrFailed, true
		}
		if val == nil {
			return nil, false, common.ErrNoKey, true
		} else {
			return val, true, common.OK, true
		}
	case common.OpPut:
		if err := db.Put(fullKey, newVal); err != nil {
			return nil, false, common.ErrFailed, true
		} else {
			return newVal, true, common.OK, true
		}
	case common.OpAppend:
		v, err := db.Get(fullKey)
		if err != nil {
			return nil, false, common.ErrFailed, true
		}
		newVal = append(v, newVal...)
		if err := db.Put(fullKey, newVal); err != nil {
			return nil, false, common.ErrFailed, true
		} else {
			return newVal, true, common.OK, true
		}
	case common.OpDelete:
		if err := db.Delete(fullKey); err != nil {
			return nil, false, common.ErrFailed, true
		} else {
			return newVal, true, common.OK, true
		}
	default:
		panic("unreconized op type")
	}
}

func (kv *ShardKV) applyKVCmdBatch(cmd KVCmd, idx int) ([]byte, bool, common.Err, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := cmd.Op
	key, newVal := op.Key, op.Value

	shardIdx := common.Key2shard(key)
	shard := kv.shardDB[shardIdx]
	if kv.currConfig.Shards[shardIdx] != kv.gid || !(shard.Status == common.SERVING || shard.Status == common.WAITING) {
		return nil, false, common.ErrWrongGroup, false
	}
	if shard.IfDuplicateAndSet(cmd.Cid, cmd.Seq, false) {
		return	nil, false, common.ErrDuplicate, false
	}
	kv.log.Debugf("KVServer %d update CkMaxSeq to %d, cmd=%v", kv.me, cmd.Seq, cmd.CmdBase)

	batch := kv.store.Batch()

	seqBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqBytes, uint64(cmd.Seq))
	batch.Put(fmt.Sprintf(ShardDupPrefix, shardIdx, cmd.Cid), seqBytes)

	db := shard.Store
	fullKey := fmt.Sprintf(ShardUserDataPattern, shard.Idx, key)
	switch op.Type {
	case common.OpPut:
		batch.Put(fullKey, newVal)
	case common.OpAppend:
		v, err := db.Get(fullKey)
		if err != nil {
			return nil, false, common.ErrFailed, true
		}
		newVal = append(v, newVal...)
		batch.Put(fullKey, newVal)
	case common.OpDelete:
		batch.Delete(fullKey)
	default:
		panic("unreconized op type")
	}

	kv.SetLastAppliedBatch(idx, batch)

	if err := batch.Execute(); err != nil {
		return nil, false, common.ErrFailed, true
	} else {
		return newVal, true, common.OK, true
	}

}

func (kv *ShardKV) applySnapshotCmd(cmd SnapshotCmd) {
	snapshotInfo := cmd.SnapInfo

	kv.log.Infof("KVServer %d received snapshot cmd: lastIncludedIdx=%d", kv.me, snapshotInfo.LastIncludedIdx)
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
			kv.shardDB[shardId] = MakeShard(shardId, common.SERVING, 0, kv.store)
		} else {

			var shard *Shard
			var ok bool

			if shard, ok = kv.shardDB[shardId]; !ok {
				shard = MakeShard(shardId, common.INVALID, 0, kv.store)
				kv.shardDB[shardId] = shard
			}
			shard.SetExOwner(currConfig.Shards[shardId])
			shard.SetStatus(common.PULLING)

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
			kv.shardDB[shardId] = MakeShard(shardId, common.SERVING, 0, kv.store)
		} else {

			var shard *Shard
			var ok bool

			if shard, ok = kv.shardDB[shardId]; !ok {
				shard = MakeShard(shardId, common.SERVING, kv.gid, kv.store)
				kv.shardDB[shardId] = shard
			}
			shard.SetExOwner(kv.gid)

			if gid == 0 {
				shard.SetStatus(common.INVALID)
			} else {
				switch shard.Status {
				case common.INVALID:
				case common.SERVING:
					// we lost a shard
					shard.SetStatus(common.ERASING)
				case common.ERASING:
				case common.PULLING:
					shard.SetStatus(common.ERASING)
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
		if localShard := kv.shardDB[shardId]; localShard.Status == common.PULLING {

			localShard.Install(shard)
			localShard.SetExOwner( kv.prevConfig.Shards[shardId])
			localShard.SetStatus(common.WAITING)
			localShard.SetVersion(kv.currConfig.Num)

			kv.log.Infof("KVServer %d shard %d installed, PULLING -> WAITING, exOwner is %d",
				kv.me, shardId, localShard.ExOwner)

		} else if localShard.Status != common.PULLING {
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
		if shard, ok := kv.shardDB[shardId]; ok && (shard.Status == common.ERASING || shard.Status == common.INVALID){
			kv.log.Infof("KVServer %d shard %d erased, ERASING -> INVALID", kv.me, shardId)
			shard.SetStatus(common.INVALID)
			if kv.currConfig.Num > shard.GetVersion() {
				shard.ClearUserData()
				kv.log.Infof("KVServer %d do clear shard data", kv.me)
			} else {
				kv.log.Infof("KVServer %d dont need to clear shard data", kv.me)
			}
		} else if shard.Status != common.ERASING {
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
		if shard, ok := kv.shardDB[shardId]; ok && shard.Status == common.WAITING {
			kv.log.Infof("KVServer %d shard %d stop waiting, WAITING -> SERVING", kv.me, shardId)
			shard.SetStatus(common.SERVING)
		} else if shard.Status != common.WAITING {
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
