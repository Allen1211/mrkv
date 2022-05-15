package master

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/Allen1211/mrkv/internal/netw"
	"github.com/Allen1211/mrkv/pkg/common"
)

type Clerk struct {
	servers []*netw.ClientEnd

	id      int64
	leader  int32
	seq     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) nextSeq() int64 {
	return atomic.AddInt64(&ck.seq, 1)
}

func (ck *Clerk) GetLeader() int {
	return int(atomic.LoadInt32(&ck.leader))
}

func (ck *Clerk) SetLeader(leader int) {
	atomic.StoreInt32(&ck.leader, int32(leader))
}

func MakeClerk(servers []*netw.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	return ck
}

func (ck *Clerk) Heartbeat(nodeId int, addr string, groups map[int]*GroupInfo) HeartbeatReply {
	args := HeartbeatArgs{}
	args.NodeId = nodeId
	args.Addr = addr
	args.Groups = groups
	args.Cid = ck.id
	args.Seq = ck.nextSeq()
	i := ck.GetLeader()
	for {
		var reply HeartbeatReply
		if ok := ck.servers[i].Call(netw.ApiHeartbeat, &args, &reply); !ok {
			// log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrFailed {
			// log.Errorf("Client %d SendRPC Err: %s", ck.id, reply.Err)
			continue
		} else if reply.Err == ErrWrongLeader || reply.WrongLeader {
			// log.Debugf("Client SendRPC Err: %d is Not Leader, try another server", i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		ck.SetLeader(i)

		return reply
	}
}

func (ck *Clerk) Query(num int) ConfigV1 {
	args := QueryArgs{}
	args.Num = num
	args.Cid = ck.id
	args.Seq = ck.nextSeq()
	i := ck.GetLeader()
	for {
		var reply QueryReply
		if ok := ck.servers[i].Call(netw.ApiQuery, &args, &reply); !ok {
			// log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrFailed {
			// log.Errorf("Client %d SendRPC Err: %s", ck.id, reply.Err)
			continue
		} else if reply.Err == ErrWrongLeader || reply.WrongLeader {
			// log.Debugf("Client SendRPC Err: %d is Not Leader, try another server", i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		ck.SetLeader(i)

		return reply.Config
	}
}

func (ck *Clerk) Join(nodes map[int][]int) common.Err {
	args := JoinArgs{}
	// Your code here.
	args.Nodes = nodes
	args.Cid = ck.id
	args.Seq = ck.nextSeq()

	i := ck.GetLeader()
	for {
		var reply JoinReply
		if ok := ck.servers[i].Call(netw.ApiJoin, &args, &reply); !ok {
			// log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrFailed {
			// log.Errorf("Client %d SendRPC Err: %s", ck.id, reply.Err)
			continue
		} else if reply.Err == ErrWrongLeader || reply.WrongLeader {
			// log.Debugf("Client SendRPC Err: %d is Not Leader, try another server", i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		ck.SetLeader(i)
		return reply.Err
	}

}

func (ck *Clerk) Leave(gids []int) common.Err {
	args := LeaveArgs{}
	args.GIDs = gids
	args.Cid = ck.id
	args.Seq = ck.nextSeq()

	i := ck.GetLeader()
	for {
		var reply LeaveReply
		if ok := ck.servers[i].Call(netw.ApiLeave, &args, &reply); !ok {
			// log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrFailed {
			// log.Errorf("Client %d SendRPC Err: %s", ck.id, reply.Err)
			i = (i + 1) % len(ck.servers)
			continue
		} else if reply.Err == ErrWrongLeader || reply.WrongLeader {
			// log.Debugf("Client SendRPC Err: %d is Not Leader, try another server", i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		ck.SetLeader(i)
		return reply.Err
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{}
	args.Shard = shard
	args.GID = gid
	args.Cid = ck.id
	args.Seq = ck.nextSeq()

	i := ck.GetLeader()
	for {
		var reply LeaveReply
		if ok := ck.servers[i].Call(netw.ApiMove, &args, &reply); !ok {
			// log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrFailed {
			// log.Errorf("Client %d SendRPC Err: %s", ck.id, reply.Err)
			continue
		} else if reply.Err == ErrWrongLeader || reply.WrongLeader {
			// log.Debugf("Client SendRPC Err: %d is Not Leader, try another server", i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		ck.SetLeader(i)
		return
	}
}



func (ck *Clerk) Show(args ShowArgs) ShowReply {
	i := ck.GetLeader()
	for {
		var reply ShowReply
		if ok := ck.servers[i].Call(netw.ApiShow, &args, &reply); !ok {
			// log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrFailed {
			// log.Errorf("Client %d SendRPC Err: %s", ck.id, reply.Err)
			continue
		} else if reply.Err == ErrWrongLeader {
			// log.Debugf("Client SendRPC Err: %d is Not Leader, try another server", i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		ck.SetLeader(i)

		return reply
	}
}

func (ck *Clerk) ShowMaster() []ShowMasterReply {
	var wg sync.WaitGroup
	res := make([]ShowMasterReply, len(ck.servers))
	wg.Add(len(ck.servers))
	for i := 0; i < len(ck.servers); i++ {
		go func(j int) {
			defer wg.Done()
			var reply ShowMasterReply
			if ok := ck.servers[j].Call(netw.ApiShowMaster, &ShowMasterArgs{}, &reply); !ok {
				// log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, j)

				reply.Status = "Disconnected"
				reply.Id = j
			}
			reply.Addr = ck.servers[j].Addr
			res[j] = reply
		}(i)
	}
	wg.Wait()
	return res
}