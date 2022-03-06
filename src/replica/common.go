package replica

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type Err string

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrFailed 	   = "ErrFailed"
	ErrDuplicate   = "ErrDuplicate"
	ErrDiffConf    = "ErrDiffConf"

	OpGet    = "GET"
	OpPut    = "PUT"
	OpAppend = "APPEND"
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

type ShardStatus int

const (
	SERVING  	ShardStatus = iota
	INVALID
	PULLING
	ERASING
	WAITING
)

type BaseArgs struct {
	Cid 	int64
	Seq 	int64
	ConfNum int
}

// Put or Append
type PutAppendArgs struct {
	BaseArgs

	Key   string
	Value string
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	BaseArgs
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	BaseArgs
	Shards  []int
}

// type PullShardReply struct {
// 	Err 		Err
// 	ConfNum 	int
// 	Shards  	map[int]*Shard
// }

type PullShardReply struct {
	Err 		Err
	ConfNum 	int
	Shards  	map[int][]byte
}

type EraseShardArgs struct {
	BaseArgs
	Shards  []int
}

type EraseShardReply struct {
	Err 		Err
	ConfNum 	int
}