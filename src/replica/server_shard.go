package replica

import (
	"encoding/binary"
	"fmt"
)

type Shard struct {
	Idx      int
	Status   ShardStatus
	ExOwner  int

	Store Store
}

func MakeShard(id int, status ShardStatus, exOwner int, store Store) *Shard {
	s := new(Shard)
	s.Store = store
	s.SetStatus(status)
	s.SetExOwner(exOwner)
	s.Idx = id
	return s
}

func (s *Shard) SetStatus(status ShardStatus) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(status))
	if err := s.Store.Put(fmt.Sprintf(ShardMetaPrefix, s.Idx, "status"), buf); err != nil {
		panic(err)
	}
	s.Status = status
}

func (s *Shard) SetExOwner(exOwner int) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(exOwner))
	if err := s.Store.Put(fmt.Sprintf(ShardMetaPrefix, s.Idx, "exOwner"), buf); err != nil {
		panic(err)
	}
	s.ExOwner = exOwner
}

func (s *Shard) IfDuplicateAndSet(cid, seq int64, set bool) bool {
	key := fmt.Sprintf(ShardDupPrefix, s.Idx, cid)
	val, err := s.Store.Get(key)
	if err != nil {
		panic(err)
	}
	if val != nil {
		maxSeq := int64(binary.LittleEndian.Uint64(val))
		if seq <= maxSeq {
			return true
		}
	}
	if set {
		val = make([]byte, 8)
		binary.LittleEndian.PutUint64(val, uint64(seq))
		if err := s.Store.Put(key, val); err != nil {
			panic(err)
		}
	}
	return false
}

func (s *Shard) Dump() []byte {
	snapshot, err := s.Store.SnapshotShard(s.Idx)
	if err != nil {
		panic(err)
	}
	return snapshot
}

func (s *Shard) Install(snapshot []byte) {
	err := s.Store.ApplySnapshot(snapshot)
	if err != nil {
		panic(err)
	}
}

func (s *Shard) LoadFromStore() (err error) {
	var val []byte
	if val, err = s.Store.Get(fmt.Sprintf(ShardMetaPrefix, s.Idx, "status")); err != nil {
		return err
	} else {
		s.Status = ShardStatus(binary.LittleEndian.Uint64(val))
	}
	if val, err = s.Store.Get(fmt.Sprintf(ShardMetaPrefix, s.Idx, "exOwner")); err != nil {
		return err
	} else {
		s.ExOwner = int(binary.LittleEndian.Uint64(val))
	}
	return err
}

func (s *Shard) ClearAll() {
	prefix := fmt.Sprintf(ShardBasePrefix, s.Idx)
	if err := s.Store.Clear(prefix); err != nil {
		panic(err)
	}
}

func (s *Shard) ClearUserData() {
	prefix := fmt.Sprintf(ShardUserDataPrefix, s.Idx)
	if err := s.Store.Clear(prefix); err != nil {
		panic(err)
	}
}