package replica

import (
	"mrkv/src/raft"
	"mrkv/src/master"
)

type Cmd interface {
	GetType() CmdType
	GetSeq() 	int64
	GetCid()	int64
}

type CmdBase struct {
	Type CmdType
	Cid  int64
	Seq  int64
}

func (c *CmdBase) GetType() CmdType {
	return c.Type
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
	Value string
}

type EmptyCmd struct {
	*CmdBase
}

type KVCmd struct {
	*CmdBase
	Op  Op
}

type SnapshotCmd struct {
	*CmdBase
	SnapInfo raft.InstallSnapshotMsg
}

type InstallShardCmd struct {
	*CmdBase
	ConfNum 	int
	Shards  	map[int][]byte
}

type EraseShardCmd struct {
	*CmdBase
	ConfNum 	int
	Shards      []int
}

type StopWaitingShardCmd struct {
	*CmdBase
	ConfNum 	int
	Shards      []int
}

type ConfCmd struct {
	*CmdBase
	Config master.Config
}

type ApplyRes interface {
	GetErr()		Err
	GetCmdType()	CmdType
	GetIdx()		int
}

type ApplyResBase struct {
	err  	Err
	cmdType CmdType
	idx int
}

func (arb *ApplyResBase) GetErr() Err {
	return arb.err
}

func (arb *ApplyResBase) GetCmdType() CmdType {
	return arb.cmdType
}

func (arb *ApplyResBase) GetIdx() int {
	return arb.idx
}

type KVCmdApplyRes struct {
	*ApplyResBase

	op  Op
	val string
	ok  bool
}

type EraseShardCmdApplyRes struct {
	*ApplyResBase
}
