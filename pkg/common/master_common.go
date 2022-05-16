package common

//go:generate msgp

const NShards = 10

type GroupStatus int

const (
	GroupJoined  = iota
	GroupServing
	GroupLeaving
	GroupRemoving
	GroupRemoved
)

func (s GroupStatus) String() string {
	switch s {
	case GroupJoined: 	return "Joined"
	case GroupServing: 	return "Serving"
	case GroupLeaving: 	return "Leaving"
	case GroupRemoving: return "Removing"
	case GroupRemoved: 	return "Removed"
	}
	return ""
}

type ShardStatus int

const (
	SERVING ShardStatus = iota
	INVALID
	PULLING
	ERASING
	WAITING
)

func (s ShardStatus) String() string {
	switch s {
	case SERVING: return "Serving"
	case INVALID: return "Invalid"
	case PULLING: return "Pulling"
	case ERASING: return "Erasing"
	case WAITING: return "Waiting"
	}
	return ""
}

func Key2shard(key string) int {
	shard := hashString(key)
	// if len(key) > 0 {
	// 	shard = int(key[0])
	// }
	shard %= NShards
	return shard
}

func hashString(s string) int {
	seed := 131
	hash := 0
	for _, c := range s {
		hash = hash*seed + int(c)
	}
	return hash
}

type NodeStatus int

const (
	NodeNormal NodeStatus = iota
	NodeDisconnect
	NodeShutdown
)

func (s NodeStatus) String() string {
	switch s {
	case NodeNormal:
		return "Normal"
	case NodeDisconnect:
		return "Disconnect"
	case NodeShutdown:
		return "Shutdown"
	}
	return ""
}

type ConfigV1 struct {
	Num	   		int
	Shards 		[NShards]int
	Groups 		map[int][]ConfigNodeGroup // gid 		=> nodeIds
	// Nodes		map[int]ConfigNode  		// nodeIds  => gids
}

type ConfigNodeGroup struct {
	NodeId		int
	RaftPeer	int
	Addr		string
}

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

type OpBase struct {
	Type  	int
	Cid 	int64
	Seq 	int64
}


type OpHeartbeatCmd struct {
	OpBase
	Args HeartbeatArgs
}

type OpJoinCmd struct {
	OpBase
	Args JoinArgs
}

type OpLeaveCmd struct {
	OpBase
	Args LeaveArgs
}

type OpMoveCmd struct {
	OpBase
	Args MoveArgs
}

type OpQueryCmd struct {
	OpBase
	Args QueryArgs
}

type OpShowCmd struct {
	OpBase
	Args ShowArgs
}

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
	Err         Err
}

type LeaveArgs struct {
	BaseArgs
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	BaseArgs
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	BaseArgs
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      ConfigV1
}

type HeartbeatArgs struct {
	BaseArgs
	NodeId		int
	Addr		string
	Groups		map[int]*GroupInfo
	MetricAddr  string
}

type HeartbeatReply struct {
	WrongLeader bool
	Err         Err
	Configs     map[int]ConfigV1 // gid => config
	PrevConfigs map[int]ConfigV1 // gid => config
	LatestConf  ConfigV1
	Nodes       map[int]NodeInfo
	Groups      map[int]GroupInfo
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
	Err    Err
	Nodes  []ShowNodeRes
	Groups []ShowGroupRes
	Shards []ShowShardRes
}

type ShowNodeRes struct {
	Found 		bool
	Id			int
	Addr		string
	Groups		[]int
	IsLeader    map[int]bool
	Status		string
	MetricAddr 	string
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
	Found      bool
	Id         int
	Gid        int
	Status     ShardStatus
	Size       int64
	Capacity   uint64
	RangeStart string
	RangeEnd   string
}

type NodeInfo struct {
	Id		int
	Addr	string
}

type GroupInfo struct {
	Id       int
	ConfNum  int
	IsLeader bool
	Status   GroupStatus
	Shards   map[int]ShardInfo
	Size     int64
	Peer     int

	RemoteConfNum int
}

type ShardInfo struct {
	Id         int
	Gid        int
	Status     ShardStatus
	Size       int64
	Capacity   uint64
	RangeStart string
	RangeEnd   string
	ExOwner    int
}

type TransferLeaderArgs struct {
	Gid    int
	Target int
}

type TransferLeaderReply struct {
	Err Err
}

type ShowMasterArgs struct {
	Dummy			int
}

type ShowMasterReply struct {
	Id				int
	Addr			string
	IsLeader		bool
	LatestConfNum	int
	Size			int64
	Status			string
	MetricAddr 		string
}