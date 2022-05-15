package replica

import (
	"encoding/binary"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/Allen1211/mrkv/pkg/common"
)

type Shard struct {
	Idx     int
	Status  common.ShardStatus
	ExOwner int

	Store Store
}

func MakeShard(id int, status common.ShardStatus, exOwner int, store Store) *Shard {
	s := new(Shard)
	s.Idx = id
	s.Store = store
	s.SetStatus(status)
	s.SetExOwner(exOwner)
	return s
}

func (s *Shard) SetStatus(status common.ShardStatus) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(status))
	if err := s.Store.Put(fmt.Sprintf(ShardMetaPrefix, s.Idx, "status"), buf); err != nil {
		fmt.Println(err)
	}
	s.Status = status
}

func (s *Shard) SetExOwner(exOwner int) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(exOwner))
	if err := s.Store.Put(fmt.Sprintf(ShardMetaPrefix, s.Idx, "exOwner"), buf); err != nil {
		fmt.Println(err)
	}
	s.ExOwner = exOwner
}

func (s *Shard) IfDuplicateAndSet(cid, seq int64, set bool) bool {
	key := fmt.Sprintf(ShardDupPrefix, s.Idx, cid)
	val, err := s.Store.Get(key)
	if err != nil {
		fmt.Println(err)
		return true
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
			fmt.Println(err)
		}
	}
	return false
}

func (s *Shard) Dump() []byte {
	snapshot, err := s.Store.SnapshotShard(s.Idx)
	if err != nil && err != leveldb.ErrClosed {
		fmt.Println(err)
	}
	return snapshot
}

func (s *Shard) Install(snapshot []byte) {
	err := s.Store.ApplySnapshot(snapshot)
	if err != nil {
		fmt.Println(err)
	}
}

func (s *Shard) SetVersion(version int)  {
	key := fmt.Sprintf(ShardVersion, s.Idx)
	val, err := s.Store.Get(key)
	if err != nil {
		fmt.Println(err)
	}
	if val != nil && len(val) > 0 {
		oldVersion := int(binary.LittleEndian.Uint64(val))
		if version <= oldVersion {
			return
		}
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(version))
	if err := s.Store.Put(key, buf); err != nil {
		fmt.Println(err)
	}
}
func (s *Shard) GetVersion() int {
	key := fmt.Sprintf(ShardVersion, s.Idx)
	val, err := s.Store.Get(key)
	if err != nil {
		fmt.Println(err)
	}
	if val != nil && len(val) > 0 {
		return int(binary.LittleEndian.Uint64(val))
	} else {
		return 0
	}
}

func (s *Shard) LoadFromStore() (err error) {
	var val []byte
	if val, err = s.Store.Get(fmt.Sprintf(ShardMetaPrefix, s.Idx, "status")); err != nil {
		return err
	} else if val != nil  {
		s.Status = common.ShardStatus(binary.LittleEndian.Uint64(val))
	}
	if val, err = s.Store.Get(fmt.Sprintf(ShardMetaPrefix, s.Idx, "exOwner")); err != nil {
		return err
	} else if val != nil {
		s.ExOwner = int(binary.LittleEndian.Uint64(val))
	}
	return err
}


func (s *Shard) ClearUserData() {
	prefix := fmt.Sprintf(ShardUserDataPrefix, s.Idx)
	if err := s.Store.Clear(prefix); err != nil {
		fmt.Println(err)
	}
}

func (s *Shard) Size() int64 {
	prefix := fmt.Sprintf(ShardBasePrefix, s.Idx)
	size, err := s.Store.Size([]string{prefix})
	if err != nil && err != leveldb.ErrClosed {
		fmt.Println(err)
	}
	return size
}