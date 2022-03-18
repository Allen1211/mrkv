package replica

import (
	"mrkv/src/netw"
)

func (kv *ShardKV) rpcFuncImpl(apiName string, args interface{}, reply interface{}, ids ...int) bool {
	b := args.(netw.IRPCArgBase)
	b.SetPeer(ids[0])
	b.SetGid(kv.gid)
	return kv.rpcFunc(apiName, args, reply, ids[0], kv.gid)
}

func (kv *ShardKV) getNodeIdByPeer(peer int) (int, bool) {
	groups := kv.currConfig.Groups[kv.gid]
	for _, group := range groups {
		if group.RaftPeer == peer {
			return  group.NodeId, true
		}
	}
	return 0, false
}