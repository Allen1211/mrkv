package replica

func (kv *ShardKV) checkpointer()  {
	cpC := kv.rf.CheckpointCh()
	for {
		select {
		case <-kv.KilledC:
			kv.log.Debugf("KVServer %d has been killed, stop checkpointer loop", kv.me)
			kv.exitedC <- runFuncName()
			return
		case cpIdx := <-(*cpC):
			kv.log.Infof("KVServer %d checkpointer receive from checkpoint channel: %d", kv.me, cpIdx)
			kv.mu.RLock()
			if cpIdx >= kv.lastApplied {
				kv.log.Infof("KVServer %d checkpointer: received cpIdx %d >= lastApplied %d, no need to checkpoint",
					kv.me, cpIdx, kv.lastApplied)
				kv.mu.RUnlock()
				continue
			}
			kv.mu.RUnlock()

			kv.doLogCompact(false, false)

			kv.mu.RLock()
			kv.log.Infof("KVServer %d call raft LogCompact finished, now lastApplied is %d",
				kv.me, kv.lastApplied)
			kv.mu.RUnlock()
		}

	}
}

func (kv *ShardKV) doLogCompact(needLock, needSnapshot bool) {
	var (
		lastIdx int
		data    []byte
		err     error
	)
	kv.mu.Lock()

	lastIdx = kv.lastApplied

	if needSnapshot {
		if data, err = kv.createSnapshot(); err != nil {
			kv.log.Errorf("KVServer %d failed to create snapshot of kv: %v", kv.me, err)
			kv.mu.Unlock()
			return
		}
	} else {
		data = []byte{}
	}

	kv.log.Infof("KVServer %d success to create snapshot, size is %d, ready to call raft.LogCompact",
		kv.me, len(data))

	kv.mu.Unlock()

	kv.rf.LogCompact(data, lastIdx, needLock)
}

func (kv *ShardKV) createSnapshot() ([]byte, error) {
	return kv.store.Snapshot()
}


func (kv *ShardKV) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	if err := kv.store.ApplySnapshot(snapshot); err != nil{
		kv.log.Errorf(err.Error())
	}
	if err := kv.RecoverFromStore(); err != nil {
		kv.log.Errorf(err.Error())
	}

	kv.log.Infof("KVServer %d apply snapshot, size is %d, lastAppliedIndex is %d", kv.me, len(snapshot), kv.lastApplied)

}