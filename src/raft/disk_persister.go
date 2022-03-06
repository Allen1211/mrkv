package raft

import (
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"

	utils "mrkv/src/common/utils"
)

const (
	snapFileName = "snapshot"
	raftStateFileName = "raftstate"
)

type DiskPersister struct {
	mu  sync.RWMutex
	dir string
	snapshotPath	string
	raftStatePath	string
}

func MakeDiskPersister(dir string) (*DiskPersister, error) {
	if err := utils.CheckAndMkdir(dir); err != nil {
		return nil, err
	}
	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	p := &DiskPersister{
		mu:            sync.RWMutex{},
		dir:           dir,
		snapshotPath:  dir + snapFileName,
		raftStatePath: dir + raftStateFileName,
	}
	return p, nil
}

func (d *DiskPersister) Copy() Persister {
	return &DiskPersister{
		mu:  sync.RWMutex{},
		dir: d.dir,
		snapshotPath: d.dir + snapFileName,
		raftStatePath: d.dir + raftStateFileName,
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
	if err := utils.CheckAndMkdir(d.dir); err != nil {
		log.Fatalf("DistPersister failed to check dir: %v\n", err)
	}
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

func (d *DiskPersister) SaveLogState(state []byte) {
}

func (d *DiskPersister) ReadLogState() []byte {
	return nil
}

func (d *DiskPersister) LogStateSize() int {
	return 0
}