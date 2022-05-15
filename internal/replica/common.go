package replica

import (
	"github.com/Allen1211/mrkv/pkg/common"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

//go:generate msgp

const (
	OpGet    = "GET"
	OpPut    = "PUT"
	OpAppend = "APPEND"
	OpDelete = "DELETE"
)

type CmdType string

const (
	CmdEmpty		= "EMPTY"
	CmdKV    		= "KV"
	CmdSnap   		= "SNAPSHOT"
	CmdConf	 		= "CONFIG"
	CmdInstallShard = "INSTALL SHARD"
	CmdEraseShard 	= "ERASE SHARD"
	CmdStopWaiting  = "STOP WAITING SHARD"
)

type BaseArgs struct {
	Cid 	int64
	Seq 	int64
	ConfNum int
	Gid		int
}

type FromReply struct {
	NodeId int
	GID    int
	Peer   int
}

// Put or Append
type PutAppendArgs struct {
	BaseArgs

	Key   string
	Value []byte
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	FromReply
	Err common.Err
}

type GetArgs struct {
	BaseArgs
	Key string
}

type GetReply struct {
	FromReply
	Err   common.Err
	Value []byte
}

type DeleteArgs struct {
	BaseArgs
	Key string
}

type DeleteReply struct {
	FromReply
	Err common.Err
}

type PullShardArgs struct {
	BaseArgs
	Shards  []int
}


type PullShardReply struct {
	Err     common.Err
	ConfNum int
	Shards  map[int][]byte
}

type EraseShardArgs struct {
	BaseArgs
	Shards  []int
}

type EraseShardReply struct {
	Err     common.Err
	ConfNum int
}