package replica

import (
	"github.com/allen1211/mrkv/internal/raft"
	"github.com/allen1211/mrkv/pkg/common"
)

//go:generate msgp

type CmdBase struct {
	Cid  int64
	Seq  int64
}

func (c *CmdBase) GetSeq() int64 {
	return c.Seq
}

func (c *CmdBase) GetCid() int64 {
	return c.Cid
}

type Op struct {
	Type  string
	Key   string
	Value []byte
}

type EmptyCmd struct {
	CmdBase
}

type KVCmd struct {
	CmdBase
	Op Op
}

type SnapshotCmd struct {
	CmdBase
	SnapInfo raft.InstallSnapshotMsg
}

type InstallShardCmd struct {
	CmdBase
	ConfNum 	int
	Shards  	map[int][]byte
}

type EraseShardCmd struct {
	CmdBase
	ConfNum 	int
	Shards      []int
}

type StopWaitingShardCmd struct {
	CmdBase
	ConfNum 	int
	Shards      []int
}

type ConfCmd struct {
	CmdBase
	Config common.ConfigV1
}

type ApplyRes interface {
	GetErr() common.Err
	GetCmdType() common.CmdType
	GetIdx()		int
}

type ApplyResBase struct {
	err     common.Err
	cmdType common.CmdType
	idx     int
}

func (arb *ApplyResBase) GetErr() common.Err {
	return arb.err
}

func (arb *ApplyResBase) GetCmdType() common.CmdType {
	return arb.cmdType
}

func (arb *ApplyResBase) GetIdx() int {
	return arb.idx
}

type KVCmdApplyRes struct {
	*ApplyResBase

	op  Op
	val []byte
	ok  bool
}

type EraseShardCmdApplyRes struct {
	*ApplyResBase
}
