package raft

import (
	"sync"

	"github.com/allen1211/mrkv/pkg/common/utils"
)

const (
	snapFileName = "snapshot"
	raftStateFileName = "raftstate"
)

type DiskPersister struct {
	mu  sync.RWMutex
	snapshotPath	string
	raftStatePath	string
}

func MakeDiskPersister(snapshotPath, raftStatePath  string) (*DiskPersister, error) {
	p := &DiskPersister{
		mu:            sync.RWMutex{},
		snapshotPath:  snapshotPath,
		raftStatePath: raftStatePath,
	}
	return p, nil
}

func (d *DiskPersister) Copy() Persister {
	return &DiskPersister{
		mu:  sync.RWMutex{},
		snapshotPath: d.snapshotPath,
		raftStatePath: d.raftStatePath,
	}
}

func (d *DiskPersister) SaveRaftState(state []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()

	utils.WriteFile(d.raftStatePath, state)
}

func (d *DiskPersister) ReadRaftState() []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return utils.ReadFile(d.raftStatePath)
}

func (d *DiskPersister) RaftStateSize() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return utils.SizeOfFile(d.raftStatePath)
}

func (d *DiskPersister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()

	utils.WriteFile(d.raftStatePath, state)
	utils.WriteFile(d.snapshotPath, snapshot)
}

func (d *DiskPersister) ReadSnapshot() []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return utils.ReadFile(d.snapshotPath)
}

func (d *DiskPersister) SnapshotSize() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return utils.SizeOfFile(d.snapshotPath)
}

func (d *DiskPersister) SaveSnapshot(snapshot []byte)  {
	d.mu.Lock()
	defer d.mu.Unlock()
	utils.WriteFile(d.snapshotPath, snapshot)
}

func (d *DiskPersister) SaveLogState(state []byte) {
}

func (d *DiskPersister) ReadLogState() []byte {
	return nil
}

func (d *DiskPersister) LogStateSize() int {
	return 0
}

func (d *DiskPersister) Clear()  {
	utils.DeleteFile(d.snapshotPath)
	utils.DeleteFile(d.raftStatePath)
}