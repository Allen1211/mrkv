package node

import (
	"errors"
	"fmt"

	"mrkv/src/common"
	"mrkv/src/netw"
	"mrkv/src/raft"
	"mrkv/src/replica"
)

func (n *Node) rpcFuncImpl(apiName string, args interface{}, reply interface{}, ids ...int) bool {
	var nodeId int
	switch apiName {
	case netw.ApiPullShard: fallthrough
	case netw.ApiEraseShard:
		nodeId = ids[0]
	case netw.ApiAppendEntries: 		fallthrough
	case netw.ApiRequestVote:			fallthrough
	case netw.ApiInstallSnapshot:		fallthrough
	case netw.ApiReadIndexFromFollower:
		nodeId = n.route(ids[1], ids[0])
	}

	nodeEnd := n.getOrCreateNodeEnd(nodeId)
	if nodeEnd == nil {
		n.logger.Errorf("can't find node %d in nodeInfos", nodeId)
		return false
	} else {
		return nodeEnd.Call(fmt.Sprintf("Node-%d.%s", nodeId, apiName), args, reply)
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

func (n *Node) Get(args *replica.GetArgs, reply *replica.GetReply) (e error) {
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.Get cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		reply.NodeId = n.Id
		return group.replica.Get(args, reply)
	}
}

func (n *Node) PutAppend(args *replica.PutAppendArgs, reply *replica.PutAppendReply) (e error) {
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.PutAppend cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		reply.NodeId = n.Id
		return group.replica.PutAppend(args, reply)
	}
}

func (n *Node) Delete(args *replica.DeleteArgs, reply *replica.DeleteReply) (e error) {
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.PutAppend cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		reply.NodeId = n.Id
		return group.replica.Delete(args, reply)
	}
}


func (n *Node) PullShard(args *replica.PullShardArgs, reply *replica.PullShardReply) (e error) {
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.PullShard cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		return group.replica.PullShard(args, reply)
	}
}

func (n *Node) EraseShard(args *replica.EraseShardArgs, reply *replica.EraseShardReply ) (e error) {
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Replica.EraseShard cannot found group %d in this node", args.Gid)
		reply.Err = common.ErrWrongGroup
		return nil
	} else {
		return group.replica.EraseShard(args, reply)
	}
}

/* API of Raft */

func (n *Node) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) (err error) {
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Raft.AppendEntries cannot found group %d in this node", args.Gid)
		return errors.New(string(common.ErrWrongGroup))
	} else {
		return group.replica.Raft().AppendEntries(args, reply)
	}
}

func (n *Node) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) (err error) {
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Raft.RequestVote cannot found group %d in this node", args.Gid)
		return errors.New(string(common.ErrWrongGroup))
	} else {
		return group.replica.Raft().RequestVote(args, reply)
	}
}

func (n *Node) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply)(err error) {
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Raft.InstallSnapshot cannot found group %d in this node", args.Gid)
		return errors.New(string(common.ErrWrongGroup))
	} else {
		return group.replica.Raft().InstallSnapshot(args, reply)
	}
}

func (n *Node) ReadIndexFromFollower(args *raft.ReadIndexFromFollowerArgs, reply *raft.ReadIndexFromFollowerReply) (err error) {
	if group, ok := n.groups[args.Gid]; !ok || group.replica == nil {
		n.logger.Errorf("RPC Call Raft.ReadIndexFromFollower cannot found group %d in this node", args.Gid)
		return errors.New(string(common.ErrWrongGroup))
	} else {
		err := group.replica.Raft().ReadIndexFromFollower(args, reply)
		fmt.Println(err)
		fmt.Println(reply)
		return err
	}
}

func (n *Node) createNodeEnd(nodeId int)  {
	node := n.nodeInfos[nodeId]
	n.nodeEnds[nodeId] = netw.MakeRPCEnd(fmt.Sprintf("Node-%d", nodeId), "tcp", node.Addr)
}

func (n *Node) getOrCreateNodeEnd(nodeId int) *netw.ClientEnd {
	node, ok := n.nodeInfos[nodeId]
	if !ok {
		return nil
	}
	if end, ok := n.nodeEnds[nodeId]; !ok {
		end = netw.MakeRPCEnd(fmt.Sprintf("Node"), "tcp", node.Addr)
		n.nodeEnds[nodeId] = end
		return end
	} else {
		return end
	}
}