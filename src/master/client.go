package master

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"

	log "github.com/sirupsen/logrus"

	"mrkv/src/netw"
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

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num
	args.Cid = ck.id
	args.Seq = ck.nextSeq()
	i := ck.GetLeader()
	for {
		var reply QueryReply
		if ok := ck.servers[i].Call(fmt.Sprintf("Master%d.Query", i), args, &reply); !ok {
			log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrFailed {
			log.Errorf("Client %d SendRPC Err: %s", ck.id, reply.Err)
			continue
		} else if reply.Err == ErrWrongLeader || reply.WrongLeader {
			log.Debugf("Client SendRPC Err: %d is Not Leader, try another server", i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		ck.SetLeader(i)

		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.Cid = ck.id
	args.Seq = ck.nextSeq()

	i := ck.GetLeader()
	for {
		var reply JoinReply
		if ok := ck.servers[i].Call(fmt.Sprintf("Master%d.Join", i), args, &reply); !ok {
			log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrFailed {
			log.Errorf("Client %d SendRPC Err: %s", ck.id, reply.Err)
			continue
		} else if reply.Err == ErrWrongLeader || reply.WrongLeader {
			log.Debugf("Client SendRPC Err: %d is Not Leader, try another server", i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		ck.SetLeader(i)
		return
	}

}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Cid = ck.id
	args.Seq = ck.nextSeq()

	i := ck.GetLeader()
	for {
		var reply LeaveReply
		if ok := ck.servers[i].Call(fmt.Sprintf("Master%d.Leave", i), args, &reply); !ok {
			log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrFailed {
			log.Errorf("Client %d SendRPC Err: %s", ck.id, reply.Err)
			continue
		} else if reply.Err == ErrWrongLeader || reply.WrongLeader {
			log.Debugf("Client SendRPC Err: %d is Not Leader, try another server", i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		ck.SetLeader(i)
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.Shard = shard
	args.GID = gid
	args.Cid = ck.id
	args.Seq = ck.nextSeq()

	i := ck.GetLeader()
	for {
		var reply LeaveReply
		if ok := ck.servers[i].Call(fmt.Sprintf("Master%d.Move", i), args, &reply); !ok {
			log.Debugf("Client %d Fail to Send RPC to server %d", ck.id, i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrFailed {
			log.Errorf("Client %d SendRPC Err: %s", ck.id, reply.Err)
			continue
		} else if reply.Err == ErrWrongLeader || reply.WrongLeader {
			log.Debugf("Client SendRPC Err: %d is Not Leader, try another server", i)
			i = (i + 1) % len(ck.servers)
			continue
		}
		ck.SetLeader(i)
		return
	}
}
