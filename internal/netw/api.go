package netw

//go:generate msgp

type RpcFunc func(apiName string, args interface{}, reply interface{}, ids ...int) bool

const (
	ApiAppendEntries = "AppendEntries"
	ApiRequestVote = "RequestVote"
	ApiInstallSnapshot = "InstallSnapshot"
	ApiReadIndexFromFollower = "ReadIndexFromFollower"
	ApiTimeoutNow = "TimeoutNow"
	ApiTransferLeader = "TransferLeader"

	ApiGet = "Get"
	ApiPutAppend = "PutAppend"
	ApiDelete = "Delete"
	ApiPullShard = "PullShard"
	ApiEraseShard = "EraseShard"

	ApiHeartbeat = "Heartbeat"
	ApiShow = "Show"
	ApiShowMaster = "ShowMaster"
	ApiQuery = "Query"
	ApiJoin = "Join"
	ApiLeave = "Leave"
	ApiMove = "Move"
)

type IRPCArgBase interface {
	GetGid() 	int
	GetPeer() 	int
	SetGid(gid int)
	SetPeer(peer int)
}

type RPCArgBase struct {
	Gid		int
	Peer	int
}

func (b *RPCArgBase) GetGid() int {
	return b.Gid
}

func (b *RPCArgBase) GetPeer() int {
	return b.Peer
}

func (b *RPCArgBase) SetGid(gid int)  {
	b.Gid = gid
}

func (b *RPCArgBase) SetPeer(peer int) {
	b.Peer = peer
}
