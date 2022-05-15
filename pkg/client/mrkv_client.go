package client

import (
	"fmt"

	"github.com/Allen1211/mrkv/internal/master"
	"github.com/Allen1211/mrkv/internal/netw"
	"github.com/Allen1211/mrkv/pkg/common"
)

type API interface {
	Get(key string) common.GetReply
	Put(key string, val []byte) common.PutAppendReply
	Append(key string, val []byte) common.PutAppendReply
	Delete(key string) common.DeleteReply

	Join(gid int, nodes []int) common.Err
	Leave(gid int) common.Err

	ShowNodes(nodeIds []int)		([]common.ShowNodeRes, common.Err)
	ShowGroups(gids []int)			([]common.ShowGroupRes, common.Err)
	ShowShards(gids []int)			([]common.ShowShardRes, common.Err)
	ShowMaster()	[]common.ShowMasterReply

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
		ends[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i),  addr)
	}
	mc := master.MakeClerk(ends)
	kvc := MakeUserClient(ends)

	return &MrKVClient{
		masters: masters,
		mc: mc,
		kvc: kvc,
	}
}

func (c *MrKVClient) Get(key string) common.GetReply {
	return c.kvc.Get(key)
}

func (c *MrKVClient) Put(key string, val []byte) common.PutAppendReply {
	return c.kvc.Put(key, val)
}

func (c *MrKVClient) Append(key string, val []byte) common.PutAppendReply {
	return c.kvc.Append(key, val)
}

func (c *MrKVClient) Delete(key string) common.DeleteReply {
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

func (c *MrKVClient) ShowNodes(nodeIds []int) ([]common.ShowNodeRes, common.Err){
	args := common.ShowArgs{
		GIDs: []int{},
		NodeIds: []int{},
		ShardIds: []int{},
	}
	args.Nodes = true
	args.NodeIds = nodeIds

	reply := c.mc.Show(args)
	return reply.Nodes, reply.Err
}

func (c *MrKVClient) ShowGroups(gids []int)	([]common.ShowGroupRes, common.Err) {
	args := common.ShowArgs{
		GIDs: []int{},
		NodeIds: []int{},
		ShardIds: []int{},
	}
	args.Groups = true
	args.GIDs = gids

	reply := c.mc.Show(args)
	return reply.Groups, reply.Err
}

func (c *MrKVClient) ShowShards(gids []int)	([]common.ShowShardRes, common.Err) {
	args := common.ShowArgs{
		GIDs: []int{},
		NodeIds: []int{},
		ShardIds: []int{},
	}
	args.Shards = true
	args.GIDs = gids

	reply := c.mc.Show(args)
	return reply.Shards, reply.Err
}

func (c *MrKVClient) ShowMaster() []common.ShowMasterReply {
	return c.mc.ShowMaster()
}

func (c *MrKVClient) TransferLeader(gid, target int) common.Err {
	return c.kvc.TransferLeader(gid, target)
}
