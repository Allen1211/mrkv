package master

import (
	"bytes"

	"github.com/Allen1211/mrkv/pkg/common"
	"github.com/Allen1211/mrkv/pkg/common/labgob"
)

type SnapshotPackage struct {
	LastApplied int
	Configs     []common.ConfigV1
	RouteConfig common.ConfigV1
	Nodes       map[int]*Node
	CkMaxSeq    map[int64]int64
}

func (sm *ShardMaster) checkpointer()  {
	cpC := sm.rf.CheckpointCh()
	for {
		select {
		case <-sm.KilledC:
			sm.log.Debugf("ShardMaster %d has been killed, stop checkpointer loop", sm.me)
			return
		case cpIdx := <-(*cpC):
			sm.log.Infof("ShardMaster %d checkpointer receive from checkpoint channel: %d", sm.me, cpIdx)
			sm.mu.RLock()
			if cpIdx >= sm.lastApplied {
				sm.log.Infof("ShardMaster %d checkpointer: received cpIdx %d >= lastApplied %d, no need to checkpoint",
					sm.me, cpIdx, sm.lastApplied)
				sm.mu.RUnlock()
				continue
			}
			sm.mu.RUnlock()

			sm.doLogCompact(false, true)

			sm.mu.RLock()
			sm.log.Infof("ShardMaster %d call raft LogCompact finished, now lastApplied is %d",
				sm.me, sm.lastApplied)
			sm.mu.RUnlock()
		}

	}
}

func (sm *ShardMaster) doLogCompact(needLock, needSnapshot bool) {
	var (
		lastIdx int
		data    []byte
		err     error
	)
	sm.mu.Lock()

	lastIdx = sm.lastApplied

	if needSnapshot {
		if data, err = sm.createSnapshot(); err != nil {
			sm.log.Errorf("ShardMaster %d failed to create snapshot of sm: %v", sm.me, err)
			sm.mu.Unlock()
			return
		}
	} else {
		data = []byte{}
	}

	sm.log.Infof("ShardMaster %d success to create snapshot, size is %d, ready to call raft.LogCompact",
		sm.me, len(data))

	sm.mu.Unlock()

	sm.rf.LogCompact(data, lastIdx, needLock)
}

func (sm *ShardMaster) createSnapshot() ([]byte, error) {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)

	pkg := SnapshotPackage{
		LastApplied: sm.lastApplied,
		Configs: 	 sm.configs,
		RouteConfig: sm.routeConfig,
		Nodes: 		 sm.nodes,
		CkMaxSeq: sm.ckMaxSeq,
	}
	if err := encoder.Encode(pkg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (sm *ShardMaster) applySnapshot(snapshot []byte) error {
	buf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buf)
	pkg := SnapshotPackage{}
	if err := decoder.Decode(&pkg); err != nil {
		return err
	}
	if pkg.LastApplied <= sm.lastApplied {
		return nil
	}
	sm.lastApplied = pkg.LastApplied
	sm.nodes = pkg.Nodes
	sm.configs = pkg.Configs
	sm.routeConfig = pkg.RouteConfig
	for cid, seq := range pkg.CkMaxSeq {
		if maxSeq, ok := sm.ckMaxSeq[cid]; !ok || seq > maxSeq {
			sm.ckMaxSeq[cid] = seq
		}
	}
	return nil
}
