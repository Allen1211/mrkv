package replica

import (
	"bytes"
	"encoding/binary"
	"time"
)

const SnapshotHeaderMagic = 0xa15c3d4e

var LenOfSnapshotHeader = binary.Size(SnapshotHeader{})

type SnapshotHeader struct {
	Magic		uint32
	ShardId 	uint32
	DataLen		uint64
	// Data		[]byte
}

func (s *SnapshotHeader) Encode() []byte {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, *s)
	return buf.Bytes()
}

func (s *SnapshotHeader) Decode(buf []byte) {
	_ = binary.Read(bytes.NewReader(buf), binary.LittleEndian, &s)
}

func (kv *ShardKV) snapshoter() {
	tick := time.Tick(time.Millisecond * 100)
	for range tick {
		if kv.Killed() {
			kv.log.Debugf("KVServer %d has been killed, stop snapshoter loop", kv.me)
			kv.exitedC <- runFuncName()
			return
		}
		currRaftStateSize := kv.persister.RaftStateSize()
		if currRaftStateSize <= kv.maxraftstate {
			continue
		}
		kv.log.Debugf("KVServer %d snapshoter detect raft state size exceed maxraftstate (%d > %d) ",
			kv.me, currRaftStateSize, kv.maxraftstate)

		kv.doLogCompact(true, false)

		kv.log.Debugf("KVServer %d call raft LogCompact finished, now RaftStateSize is %d",
			kv.me, kv.persister.RaftStateSize())

	}
}

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
	// snapshot := new(bytes.Buffer)
	// for shardId, shard := range kv.shardDB {
	// 	data := shard.Dump()
	// 	header := SnapshotHeader{
	// 		Magic: 		SnapshotHeaderMagic,
	// 		ShardId: 	uint32(shardId),
	// 		DataLen:    uint64(len(data)),
	// 	}
	// 	buf := header.Encode()
	// 	buf = append(buf, data...)
	//
	// 	snapshot.Write(buf)
	// }
	// return snapshot.Bytes(), nil

	return kv.store.Snapshot()
}


func (kv *ShardKV) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	if err := kv.store.ApplySnapshot(snapshot); err != nil {
		panic(err)
	}
	if err := kv.RecoverFromStore(); err != nil {
		panic(err)
	}

	kv.log.Infof("KVServer %d apply snapshot, size is %d, lastAppliedIndex is %d", kv.me, len(snapshot), kv.lastApplied)

	// header := SnapshotHeader{}
	// for i := 0; i < len(snapshot); {
	// 	header.Decode(snapshot[i:i+LenOfSnapshotHeader])
	// 	i += LenOfSnapshotHeader
	// 	if header.Magic != SnapshotHeaderMagic {
	// 		panic("snapshot magic not match")
	// 	}
	//
	// 	dataLen, shardId := int(header.DataLen), int(header.ShardId)
	// 	data := snapshot[i:i+dataLen]
	// 	i += dataLen
	//
	// 	shard := MakeShard(shardId, INVALID, 0, kv.store)
	// 	shard.Install(data)
	// 	kv.shardDB[shardId] = shard
	// }

}