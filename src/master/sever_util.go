package master

func (sm *ShardMaster) createGroup2Nodes() map[int][]int {
	res := map[int][]int{}
	for _, node := range sm.nodes {
		for gid := range node.Groups {
			if _, ok := res[gid]; !ok {
				res[gid] = []int{}
			}
			res[gid] = append(res[gid], node.Id)
		}
	}
	// currConf := sm.getLatestConfig()
	// for gid, ngs := range currConf.Groups {
	// 	if _, ok := res[gid]; !ok {
	// 		res[gid] = []int{}
	// 	}
	// 	for _, ng := range ngs {
	// 		res[gid] = append(res[gid], ng.NodeId)
	// 	}
	// }
	return res
}

func (sm *ShardMaster) getLatestConfig() ConfigV1 {
	latestConfig := sm.configs[len(sm.configs)-1]
	return latestConfig
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

func (sm *ShardMaster) deepCopyConfMap(conf ConfigV1) map[int][]ConfigNodeGroup {
	res := make(map[int][]ConfigNodeGroup)
	for gid, servers := range conf.Groups {
		res[gid] = make([]ConfigNodeGroup, len(servers))
		copy(res[gid], servers)
	}
	return res
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