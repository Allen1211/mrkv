package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/Allen1211/mrkv/internal/netw"
	"github.com/Allen1211/mrkv/internal/raft"
	"github.com/Allen1211/mrkv/internal/replica"
	"github.com/Allen1211/mrkv/pkg/common"
)

func (n *Node) StartRPCServer() error {
	name := fmt.Sprintf("Node-%d", n.Id)
	rpcServ := netw.MakeRpcxServer(name, n.Addr())
	if err := rpcServ.Register(name, n); err != nil {
		return err
	}
	n.rpcServ = rpcServ
	go func() {
		if err := rpcServ.Start(); err != nil {
			n.logger.Errorf("%v", err)
		}
	}()

	return nil
}

func (n *Node) rpcFuncImpl(apiName string, args interface{}, reply interface{}, ids ...int) bool {
	var nodeId int
	switch apiName {
	case netw.ApiPullShard: fallthrough
	case netw.ApiEraseShard:
		nodeId = ids[0]
	case netw.ApiAppendEntries: 		fallthrough
	case netw.ApiRequestVote:			fallthrough
	case netw.ApiInstallSnapshot:		fallthrough
	case netw.ApiReadIndexFromFollower: fallthrough
	case netw.ApiTransferLeader:		fallthrough
	case netw.ApiTimeoutNow:

		nodeId = n.route(ids[1], ids[0])
	}

	nodeEnd := n.getOrCreateNodeEnd(nodeId)
	if nodeEnd == nil {
		n.logger.Errorf("can't find node %d in nodeInfos", nodeId)
		return false
	} else {
		return nodeEnd.Call(apiName, args, reply)
	}
}

func (n *Node) route(gid, peer int) int {
	ngs := n.routeConf.Groups[gid]
	for _, ng := range ngs {
		if ng.RaftPeer == peer {
			return ng.NodeId
		}
	}
	return -1
}

/* API of Replica */

func (n *Node) Get(ctx context.Context, args *replica.GetArgs, reply *replica.GetReply) (e error) {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}

	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.Get cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		reply.NodeId = n.Id
		return group.replica.Get(args, reply)
	}
}

func (n *Node) PutAppend(ctx context.Context, args *replica.PutAppendArgs, reply *replica.PutAppendReply) (e error) {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.PutAppend cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		reply.NodeId = n.Id
		return group.replica.PutAppend(args, reply)
	}
}

func (n *Node) Delete(ctx context.Context, args *replica.DeleteArgs, reply *replica.DeleteReply) (e error) {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.PutAppend cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		reply.NodeId = n.Id
		return group.replica.Delete(args, reply)
	}
}


func (n *Node) PullShard(ctx context.Context, args *replica.PullShardArgs, reply *replica.PullShardReply) (e error) {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.PullShard cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		return group.replica.PullShard(args, reply)
	}
}

func (n *Node) EraseShard(ctx context.Context, args *replica.EraseShardArgs, reply *replica.EraseShardReply) (e error) {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.EraseShard cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		return group.replica.EraseShard(args, reply)
	}
}

func (n *Node) TransferLeader(ctx context.Context, args *raft.TransferLeaderArgs, reply *raft.TransferLeaderReply) (e error)  {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.EraseShard cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		return group.replica.TransferLeader(args, reply)
	}
}


/* API of Raft */

func (n *Node) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) (err error) {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Raft.AppendEntries cannot found group %d in this node", args.Gid)
		return errors.New(string(common.ErrWrongGroup))
	} else {
		return group.replica.Raft().AppendEntries(args, reply)
	}
}

func (n *Node) RequestVote(ctx context.Context, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) (err error) {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Raft.RequestVote cannot found group %d in this node", args.Gid)
		return errors.New(string(common.ErrWrongGroup))
	} else {
		return group.replica.Raft().RequestVote(args, reply)
	}
}

func (n *Node) InstallSnapshot(ctx context.Context, args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply)(err error) {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Raft.InstallSnapshot cannot found group %d in this node", args.Gid)
		return errors.New(string(common.ErrWrongGroup))
	} else {
		return group.replica.Raft().InstallSnapshot(args, reply)
	}
}

func (n *Node) ReadIndexFromFollower(ctx context.Context, args *raft.ReadIndexFromFollowerArgs, reply *raft.ReadIndexFromFollowerReply) (err error) {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Raft.ReadIndexFromFollower cannot found group %d in this node", args.Gid)
		return errors.New(string(common.ErrWrongGroup))
	} else {
		err := group.replica.Raft().ReadIndexFromFollower(args, reply)
		return err
	}
}

func (n *Node) TimeoutNow(ctx context.Context, args *raft.TimeoutNowArgs, reply *raft.TimeoutNowReply)(err error) {
	if n.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Raft.Timeoutnow cannot found group %d in this node", args.Gid)
		return errors.New(string(common.ErrWrongGroup))
	} else {
		return group.replica.Raft().TimeoutNow(args, reply)
	}
}

func (n *Node) createNodeEnd(nodeId int)  {
	node := n.nodeInfos[nodeId]
	n.nodeEnds[nodeId] = netw.MakeRPCEnd(fmt.Sprintf("Node-%d", nodeId), node.Addr)
}

func (n *Node) getOrCreateNodeEnd(nodeId int) *netw.ClientEnd {
	n.mu.RLock()
	defer n.mu.RUnlock()

	node, ok := n.nodeInfos[nodeId]
	if !ok {
		return nil
	}
	if end, ok := n.nodeEnds[nodeId]; !ok {
		end = netw.MakeRPCEnd(fmt.Sprintf("Node-%d", nodeId),  node.Addr)
		n.nodeEnds[nodeId] = end
		return end
	} else {
		return end
	}
}