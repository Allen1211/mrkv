package master

import (
	"mrkv/src/common"
)


const NShards = 10

type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type ConfigV1 struct {
	Num	   		int
	Shards 		[NShards]int
	Groups 		map[int][]ConfigNodeGroup		// gid 		=> nodeIds
	// Nodes		map[int]ConfigNode  		// nodeIds  => gids

}

type ConfigNodeGroup struct {
	NodeId		int
	RaftPeer	int
	Addr		string
}

const (
	OK = "OK"
	ErrWrongLeader = "wrong leader"
	ErrFailed = "failed"
	ErrNodeNotRegister = "node not register"
	ErrGroupNotServing = "group is not serving"
)

const (
	OpJoin = iota
	OpLeave
	OpMove
	OpQuery
	OpHeartbeat
	OpShow
	OpRemove
	NumOfOp
)


type Op interface {
	GetType() 	int
	GetSeq() 	int64
	GetCid()	int64
}

type OpBase struct {
	Type  	int
	Cid 	int64
	Seq 	int64
}

func (o *OpBase) GetType() int {
	return o.Type
}

func (o *OpBase) GetSeq() int64 {
	return o.Seq
}

func (o *OpBase) GetCid() int64 {
	return o.Cid
}

type OpHeartbeatCmd struct {
	*OpBase
	Args  HeartbeatArgs
}

type OpJoinCmd struct {
	*OpBase
	Args JoinArgs
}

type OpLeaveCmd struct {
	*OpBase
	Args LeaveArgs
}

type OpMoveCmd struct {
	*OpBase
	Args MoveArgs
}

type OpQueryCmd struct {
	*OpBase
	Args QueryArgs
}

type OpShowCmd struct {
	*OpBase
	Args ShowArgs
}

type OpRemoveCmd struct {
	*OpBase
	Args RemoveArgs
}

type Err string

type BaseArgs struct {
	Cid int64
	Seq int64
}

type JoinArgs struct {
	BaseArgs
	// Nodes map[int][]string // new GID -> servers mappings
	Nodes map[int][]int // new GID -> nodeIds
}

type JoinReply struct {
	WrongLeader bool
	Err         common.Err
}

type LeaveArgs struct {
	BaseArgs
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         common.Err
}

type MoveArgs struct {
	BaseArgs
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         common.Err
}

type QueryArgs struct {
	BaseArgs
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         common.Err
	Config      ConfigV1
}

type RemoveArgs struct {
	BaseArgs
	Num			int
	GIDs   		[]int
}

type RemoveReply struct {
	Err         common.Err
}

type HeartbeatArgs struct {
	BaseArgs
	NodeId		int
	Addr		string
	Groups		map[int]*GroupInfo
}

type HeartbeatReply struct {
	WrongLeader bool
	Err         common.Err
	Configs			map[int]ConfigV1		// gid => config
	PrevConfigs		map[int]ConfigV1		// gid => config
	LatestConf  ConfigV1
	Nodes		map[int]NodeInfo
	Groups		map[int]GroupInfo
}

type ShowArgs struct {
	Nodes	bool
	Groups	bool
	Shards	bool

	NodeIds		[]int
	GIDs		[]int
	ShardIds 	[]int
}

type ShowReply struct {
	Err         common.Err
	Nodes		[]ShowNodeRes
	Groups      []ShowGroupRes
	Shards      []ShowShardRes
}

type ShowNodeRes struct {
	Found 		bool
	Id			int
	Addr		string
	Groups		[]int
	IsLeader    map[int]bool
	Status		string
}

type ShowGroupRes struct {
	Found 		bool
	Id			int
	ShardCnt	int

	ByNode      []ShowGroupInfoByNode
}

type ShowGroupInfoByNode struct {
	Id			int
	Addr		string
	Peer		int
	ConfNum		int
	IsLeader	bool
	Status		string
	Size		int64
}

type ShowShardRes struct {
	Found 		bool
	Id			int
	Gid			int
	Status		ShardStatus
	Size		int64
	Capacity	uint64
	RangeStart	string
	RangeEnd	string
}

type NodeInfo struct {
	Id		int
	Addr	string
}

type GroupInfo struct {
	Id			int
	ConfNum		int
	IsLeader	bool
	Status		GroupStatus
	Shards		map[int]ShardInfo
	Size		int64
	Peer		int
}

type ShardInfo struct {
	Id			int
	Gid			int
	Status		ShardStatus
	Size		int64
	Capacity	uint64
	RangeStart	string
	RangeEnd	string
}

type TransferLeaderArgs struct {
	Gid    int
	Target int
}

type TransferLeaderReply struct {
	Err		common.Err
}

type ShowMasterArgs struct {

}

type ShowMasterReply struct {
	Id				int
	Addr			string
	IsLeader		bool
	LatestConfNum	int
	Size			int64
	Status			string
}