package master

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"mrkv/src/raft"
)

func (sm *ShardMaster) StartRPCServer() error {
	if err := rpc.RegisterName(fmt.Sprintf("Master%d", sm.me) , sm); err != nil {
		return err
	}
	l, err := net.Listen(sm.servers[sm.me].Network, sm.servers[sm.me].Addr)
	if err != nil {
		return err
	}
	go func() {
		if err := http.Serve(l, nil); err != nil {
			panic(err)
		}

	}()
	return nil
}

func (sm *ShardMaster) rpcFunc(apiName string, args interface{}, reply interface{}, ids ...int) bool {
	peer := ids[0]
	return sm.servers[peer].Call(fmt.Sprintf("Master%d.%s", peer, apiName), args, reply)
}

func (sm *ShardMaster) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) (err error) {
	return sm.rf.AppendEntries(args, reply)
}

func (sm *ShardMaster) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) (err error) {
	return sm.rf.RequestVote(args, reply)
}

func (sm *ShardMaster) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply)(err error) {
	return sm.rf.InstallSnapshot(args, reply)
}

func (sm *ShardMaster) ReadIndexFromFollower(args *raft.ReadIndexFromFollowerArgs, reply *raft.ReadIndexFromFollowerReply) (err error) {
	return sm.rf.ReadIndexFromFollower(args, reply)
}


