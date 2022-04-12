package client

import (
	"fmt"

	"mrkv/src/common"
	"mrkv/src/master"
	"mrkv/src/netw"
	"mrkv/src/replica"
)

type API interface {
	Get(key string) 				replica.GetReply
	Put(key string, val []byte) 	replica.PutAppendReply
	Append(key string, val []byte) 	replica.PutAppendReply
	Delete(key string)				replica.DeleteReply

	Join(gid int, nodes []int)		common.Err
	Leave(gid int)					common.Err

	ShowNodes(nodeIds []int)		([]master.ShowNodeRes, common.Err)
	ShowGroups(gids []int)			([]master.ShowGroupRes, common.Err)
	ShowShards(gids []int)			([]master.ShowShardRes, common.Err)
	ShowMaster()	[]master.ShowMasterReply

	TransferLeader(gid int, target int) common.Err
}


type MrKVClient struct {
	masters		[]string
	mc		 	*master.Clerk
	kvc			*KvClient
}

func MakeMrKVClient(masters []string) *MrKVClient {
	ends := make([]*netw.ClientEnd, len(masters))
	for i, addr := range masters {
		ends[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), "tcp", addr)
	}
	mc := master.MakeClerk(ends)
	kvc := MakeUserClient(ends)

	return &MrKVClient {
		masters: masters,
		mc: mc,
		kvc: kvc,
	}
}

func (c *MrKVClient) Get(key string) replica.GetReply {
	return c.kvc.Get(key)
}

func (c *MrKVClient) Put(key string, val []byte) replica.PutAppendReply {
	return c.kvc.Put(key, val)
}

func (c *MrKVClient) Append(key string, val []byte) replica.PutAppendReply {
	return c.kvc.Append(key, val)
}

func (c *MrKVClient) Delete(key string) replica.DeleteReply {
	return c.kvc.Delete(key)
}

func (c *MrKVClient) Join(gid int, nodes []int) common.Err {
	return c.mc.Join(map[int][]int{
		gid: nodes,
	})
}

func (c *MrKVClient) Leave(gid int) common.Err {
	return c.mc.Leave([]int{gid})
}

func (c *MrKVClient) ShowNodes(nodeIds []int) ([]master.ShowNodeRes, common.Err){
	args := master.ShowArgs{}
	args.Nodes = true
	args.NodeIds = nodeIds

	reply := c.mc.Show(args)
	return reply.Nodes, reply.Err
}

func (c *MrKVClient) ShowGroups(gids []int)	([]master.ShowGroupRes, common.Err) {
	args := master.ShowArgs{}
	args.Groups = true
	args.GIDs = gids

	reply := c.mc.Show(args)
	return reply.Groups, reply.Err
}

func (c *MrKVClient) ShowShards(gids []int)	([]master.ShowShardRes, common.Err) {
	args := master.ShowArgs{}
	args.Shards = true
	args.GIDs = gids

	reply := c.mc.Show(args)
	return reply.Shards, reply.Err
}

func (c *MrKVClient) ShowMaster() []master.ShowMasterReply {
	return c.mc.ShowMaster()
}

func (c *MrKVClient) TransferLeader(gid, target int) common.Err {
	return c.kvc.TransferLeader(gid, target)
}
