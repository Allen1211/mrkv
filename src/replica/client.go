package replica

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the master to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mrkv/src/master"
	"mrkv/src/netw"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= master.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *master.Clerk
	config   master.Config
	make_end func(string) *netw.ClientEnd

	id      int64
	leader  int32
	seq     int64
	leaders sync.Map

	ends    map[string]*netw.ClientEnd
}

func (ck *Clerk) nextSeq() int64 {
	return atomic.AddInt64(&ck.seq, 1)
}

func (ck *Clerk) GetLeader(shardId int) int {
	if val, ok := ck.leaders.Load(shardId); ok {
		return val.(int)
	} else {
		ck.SetLeader(shardId, 0)
		return 0
	}
}

func (ck *Clerk) SetLeader(shardId, leader int) {
	ck.leaders.Store(shardId, leader)
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call master.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a netw.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*netw.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = master.MakeClerk(masters)
	ck.id = nrand()
	ck.leaders = sync.Map{}
	ck.ends = make(map[string]*netw.ClientEnd)
	return ck
}

func (ck *Clerk) getEnd(server string, gid int, i int) *netw.ClientEnd {
	if end, ok := ck.ends[server]; ok {
		return end
	}
	end := netw.MakeRPCEnd(fmt.Sprintf("Replica-%d-%d", gid, i), "tcp", server)
	ck.ends[server] = end
	return end
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	var args GetArgs
	args.Key = key
	args.Seq = ck.nextSeq()
	args.Cid = ck.id
	for {
		args.ConfNum = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			i := ck.GetLeader(shard)
			for j := 0; j < len(servers); j++ {
				srv := ck.getEnd(servers[i], gid, i)
				var reply GetReply
				if ok := srv.Call(fmt.Sprintf("Replica-%d-%d.Get", gid, i), &args, &reply); !ok {
					// DPrintf("Client %d Fail to Send RPC to server %d\n", ck.id, i)
					i = (i + 1) % len(servers)
					continue
				}
				if reply.Err == ErrFailed {
					// DPrintf("Client %d SendRPC Err: %s\n", ck.id, reply.Err)
					continue
				} else if reply.Err == ErrWrongLeader {
					// DPrintf("Client SendRPC Err: %d is Not Leader, try another server\n", i)
					i = (i + 1) % len(servers)
					continue
				} else if reply.Err == ErrWrongGroup {
					// log.Debugf("Client %d SendRPC Err: %d wrong group, re-fetch currConfig and try again\n", ck.id, gid)
					break
				}
				ck.SetLeader(shard, i)

				return reply.Value
			}
		}
		time.Sleep(50 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = strings.ToUpper(op)
	args.Seq = ck.nextSeq()
	args.Cid = ck.id
	for {
		args.ConfNum = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			i := ck.GetLeader(shard)
			for j := 0; j < len(servers); j++ {
				srv := ck.getEnd(servers[i], gid, i)
				var reply GetReply
				if ok := srv.Call(fmt.Sprintf("Replica-%d-%d.PutAppend", gid, i), &args, &reply); !ok {
					// DPrintf("Client %d Fail to Send RPC to server %d\n", ck.id, i)
					i = (i + 1) % len(servers)
					continue
				}
				if reply.Err == ErrFailed {
					// DPrintf("Client %d SendRPC Err: %s\n", ck.id, reply.Err)
					i = (i + 1) % len(servers)
					continue
				} else if reply.Err == ErrWrongLeader {
					// DPrintf("Client SendRPC Err: %d is Not Leader, try another server\n", i)
					i = (i + 1) % len(servers)
					continue
				} else if reply.Err == ErrWrongGroup {
					// DPrintf("Client %d SendRPC Err: %d wrong group, re-fetch currConfig and try again\n", ck.id, gid)
					break
				} else if reply.Err == ErrDuplicate {
					// DPrintf("Client %d SendRPC Err: duplicate\n",ck.id)

				}
				ck.SetLeader(shard, i)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
