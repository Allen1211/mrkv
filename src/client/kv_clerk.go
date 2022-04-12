package client

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mrkv/src/common"
	"mrkv/src/master"
	"mrkv/src/netw"
	"mrkv/src/raft"
	"mrkv/src/replica"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type KvClient struct {
	sm       *master.Clerk
	config   master.ConfigV1
	make_end func(string) *netw.ClientEnd

	id      int64
	leader  int32
	seq     int64
	leaders sync.Map

	rrIdx 		int
	ends    map[int]*netw.ClientEnd
}

func (ck *KvClient) nextSeq() int64 {
	return atomic.AddInt64(&ck.seq, 1)
}

func (ck *KvClient) GetLeader(shardId int) int {
	if val, ok := ck.leaders.Load(shardId); ok {
		return val.(int)
	} else {
		ck.SetLeader(shardId, 0)
		return 0
	}
}

func (ck *KvClient) SetLeader(shardId, leader int) {
	ck.leaders.Store(shardId, leader)
}

func MakeUserClient(masters []*netw.ClientEnd) *KvClient {
	ck := new(KvClient)
	ck.sm = master.MakeClerk(masters)
	ck.id = nrand()
	ck.leaders = sync.Map{}
	ck.ends = make(map[int]*netw.ClientEnd)
	return ck
}

func (ck *KvClient) getEnd(nodeId int, addr string) *netw.ClientEnd {
	if end, ok := ck.ends[nodeId]; ok {
		return end
	}
	end := netw.MakeRPCEnd("Node", "tcp", addr)
	ck.ends[nodeId] = end
	return end
}

func (ck *KvClient) getCallName(api string, nodeId int) string {
	return fmt.Sprintf("Node-%d.%s", nodeId, api)
}

func (ck *KvClient) Get(key string) replica.GetReply  {
	var args replica.GetArgs
	args.Key = key
	args.Seq = ck.nextSeq()
	args.Cid = ck.id
	for {
		args.ConfNum = ck.config.Num
		shard := master.Key2shard(key)
		gid := ck.config.Shards[shard]
		args.Gid = gid
		if servers, ok := ck.config.Groups[gid]; ok {
			for j := 0; j < len(servers); j++ {
				ck.rrIdx = (ck.rrIdx + 1) % len(servers)
				if ck.rrIdx >= len(servers) {
					continue
				}
				srv := ck.getEnd(servers[ck.rrIdx].NodeId, servers[ck.rrIdx].Addr)
				var reply replica.GetReply
				if ok := srv.Call(ck.getCallName(netw.ApiGet, servers[ck.rrIdx].NodeId), &args, &reply); !ok {
					// log.Debugf("Client %d Fail to Send RPC to server %d\n", ck.id, ck.rrIdx)
					continue
				}
				if reply.Err == common.ErrFailed || reply.Err == common.ErrNodeClosed {
					// log.Debugf("Client %d SendRPC Err: %s\n", ck.id, reply.Err)
					continue
				} else if reply.Err == common.ErrWrongLeader {
					// fmt.Printf("Client SendRPC Err: %d is Not Leader, try another server\n", ck.rrIdx)
					continue
				} else if reply.Err == common.ErrWrongGroup {
					// log.Debugf("Client %d SendRPC Err: %d wrong group, re-fetch currConfig and try again\n", ck.id, gid)
					break
				}
				return reply
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *KvClient) PutAppend(key string, value []byte, op string) replica.PutAppendReply {
	var args replica.PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = strings.ToUpper(op)
	args.Seq = ck.nextSeq()
	args.Cid = ck.id
	for {
		args.ConfNum = ck.config.Num
		shard := master.Key2shard(key)
		gid := ck.config.Shards[shard]
		args.Gid = gid

		if servers, ok := ck.config.Groups[gid]; ok {
			i := ck.GetLeader(shard)
			for j := 0; j < len(servers); j++ {
				srv := ck.getEnd(servers[i].NodeId, servers[i].Addr)
				var reply replica.PutAppendReply
				if ok := srv.Call(ck.getCallName(netw.ApiPutAppend, servers[i].NodeId), &args, &reply); !ok {
					// log.Debugf("Client %d Fail to Send RPC to server %d\n", ck.id, i)
					i = (i + 1) % len(servers)
					continue
				}
				if reply.Err == common.ErrFailed || reply.Err == common.ErrNodeClosed  {
					// log.Debugf("Client %d SendRPC Err: %s\n", ck.id, reply.Err)
					i = (i + 1) % len(servers)
					continue
				} else if reply.Err == common.ErrWrongLeader {
					// log.Debugf("Client SendRPC Err: %d is Not Leader, try another server\n", i)
					i = (i + 1) % len(servers)
					continue
				} else if reply.Err == common.ErrWrongGroup {
					// log.Debugf("Client %d SendRPC Err: %d wrong group, re-fetch currConfig and try again\n", ck.id, gid)
					break
				} else if reply.Err == common.ErrDuplicate {
					// log.Debugf("Client %d SendRPC Err: duplicate\n",ck.id)
					reply.Err = common.OK
				}
				ck.SetLeader(shard, i)
				return reply
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *KvClient) Put(key string, value []byte) replica.PutAppendReply {
	return ck.PutAppend(key, value, "Put")
}
func (ck *KvClient) Append(key string, value []byte) replica.PutAppendReply {
	return ck.PutAppend(key, value, "Append")
}

func (ck *KvClient) Delete(key string) replica.DeleteReply {
	var args replica.DeleteArgs
	args.Key = key
	args.Seq = ck.nextSeq()
	args.Cid = ck.id
	for {
		args.ConfNum = ck.config.Num
		shard := master.Key2shard(key)
		gid := ck.config.Shards[shard]
		args.Gid = gid

		if servers, ok := ck.config.Groups[gid]; ok {
			i := ck.GetLeader(shard)
			for j := 0; j < len(servers); j++ {
				srv := ck.getEnd(servers[i].NodeId, servers[i].Addr)
				var reply replica.DeleteReply
				if ok := srv.Call(ck.getCallName(netw.ApiDelete, servers[i].NodeId), &args, &reply); !ok {
					// log.Debugf("Client %d Fail to Send RPC to server %d\n", ck.id, i)
					i = (i + 1) % len(servers)
					continue
				}
				if reply.Err == common.ErrFailed || reply.Err == common.ErrNodeClosed  {
					// log.Debugf("Client %d SendRPC Err: %s\n", ck.id, reply.Err)
					i = (i + 1) % len(servers)
					continue
				} else if reply.Err == common.ErrWrongLeader {
					// log.Debugf("Client SendRPC Err: %d is Not Leader, try another server\n", i)
					i = (i + 1) % len(servers)
					continue
				} else if reply.Err == common.ErrWrongGroup {
					// log.Debugf("Client %d SendRPC Err: %d wrong group, re-fetch currConfig and try again\n", ck.id, gid)
					break
				} else if reply.Err == common.ErrDuplicate {
					// log.Debugf("Client %d SendRPC Err: duplicate\n",ck.id)
					reply.Err = common.OK
				}
				ck.SetLeader(shard, i)
				return reply
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *KvClient) TransferLeader(gid, target int) common.Err {
	args := raft.TransferLeaderArgs {
		RPCArgBase: &netw.RPCArgBase {
			Gid:  gid,
		},
		Gid:    gid,
		NodeId: target,
	}
	reply := raft.TransferLeaderReply{}

	for {
		ck.config = ck.sm.Query(-1)

		if servers, ok := ck.config.Groups[gid]; ok {
			for j := 0; j < len(servers); j++ {
				args.Peer = j
				srv := ck.getEnd(servers[j].NodeId, servers[j].Addr)
				if ok := srv.Call(ck.getCallName(netw.ApiTransferLeader, servers[j].NodeId), &args, &reply); !ok {
					// log.Debugf("Client %d Fail to Send RPC to server %d\n", ck.id, j)
					continue
				}
				if reply.Err == common.ErrWrongLeader {
					// log.Debugf("Client SendRPC Err: %d is Not Leader, try another server\n", j)
					continue
				}
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}