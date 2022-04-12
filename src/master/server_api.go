package master

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"

	"mrkv/src/common"
	"mrkv/src/raft"
)

func (sm *ShardMaster) StartRPCServer() error {
	server := rpc.NewServer()
	if err := server.RegisterName(fmt.Sprintf("Master%d", sm.me) , sm); err != nil {
		return err
	}
	l, err := net.Listen(sm.servers[sm.me].Network, sm.servers[sm.me].Addr)
	if err != nil {
		return err
	}
	sm.listener = l
	go func() {
		for !sm.Killed() {
			conn, err2 := l.Accept()
			if err2 != nil {
				continue
			}
			go server.ServeConn(conn)
		}
	}()
	return nil
}

func (sm *ShardMaster) rpcFunc(apiName string, args interface{}, reply interface{}, ids ...int) bool {
	peer := ids[0]
	return sm.servers[peer].Call(fmt.Sprintf("Master%d.%s", peer, apiName), args, reply)
}

func (sm *ShardMaster) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) (err error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	return sm.rf.AppendEntries(args, reply)
}

func (sm *ShardMaster) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) (err error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	return sm.rf.RequestVote(args, reply)
}

func (sm *ShardMaster) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply)(err error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	return sm.rf.InstallSnapshot(args, reply)
}

func (sm *ShardMaster) ReadIndexFromFollower(args *raft.ReadIndexFromFollowerArgs, reply *raft.ReadIndexFromFollowerReply) (err error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	return sm.rf.ReadIndexFromFollower(args, reply)
}


