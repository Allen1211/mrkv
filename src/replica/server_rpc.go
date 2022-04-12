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