package common

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

type BaseArgsN struct {
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
	BaseArgsN

	Key   string
	Value []byte
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	FromReply
	Err Err
}

type GetArgs struct {
	BaseArgsN
	Key string
}

type GetReply struct {
	FromReply
	Err   Err
	Value []byte
}

type DeleteArgs struct {
	BaseArgsN
	Key string
}

type DeleteReply struct {
	FromReply
	Err Err
}

type PullShardArgs struct {
	BaseArgsN
	Shards  []int
}


type PullShardReply struct {
	Err     Err
	ConfNum int
	Shards  map[int][]byte
}

type EraseShardArgs struct {
	BaseArgsN
	Shards  []int
}

type EraseShardReply struct {
	Err     Err
	ConfNum int
}