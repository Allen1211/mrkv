package master


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
	SERVING  	ShardStatus = iota
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
	case NodeNormal: return "Normal"
	case NodeDisconnect: return "Disconnect"
	case NodeShutdown: return "Shutdown"
	}
	return ""
}