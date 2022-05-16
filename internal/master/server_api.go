package master

import (
	"context"
	"errors"
	"fmt"

	"github.com/allen1211/mrkv/internal/netw"
	"github.com/allen1211/mrkv/internal/raft"
	"github.com/allen1211/mrkv/pkg/common"
)

func (sm *ShardMaster) StartRPCServer() error {
	name := fmt.Sprintf("Master%d", sm.me)
	rpcServ := netw.MakeRpcxServer(name, sm.servers[sm.me].Addr)
	if err := rpcServ.Register(name, sm); err != nil {
		return err
	}
	sm.rpcServ = rpcServ
	go func() {
		if err := rpcServ.Start(); err != nil {
			sm.log.Errorf("%v", err)
		}
	}()

	return nil
}

func (sm *ShardMaster) rpcFunc(apiName string, args interface{}, reply interface{}, ids ...int) bool {
	peer := ids[0]
	return sm.servers[peer].Call(apiName, args, reply)
}

func (sm *ShardMaster) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) (err error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	return sm.rf.AppendEntries(args, reply)
}

func (sm *ShardMaster) RequestVote(ctx context.Context, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) (err error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	return sm.rf.RequestVote(args, reply)
}

func (sm *ShardMaster) InstallSnapshot(ctx context.Context, args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply)(err error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	return sm.rf.InstallSnapshot(args, reply)
}

func (sm *ShardMaster) ReadIndexFromFollower(ctx context.Context, args *raft.ReadIndexFromFollowerArgs, reply *raft.ReadIndexFromFollowerReply) (err error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	return sm.rf.ReadIndexFromFollower(args, reply)
}


