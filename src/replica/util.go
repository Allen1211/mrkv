package replica

import (
	"runtime"
	"time"

	"mrkv/src/master"
)

func willTimeout(f func(), timeout time.Duration) bool {
	done := make(chan int, 1)
	go func() {
		f()
		done <- 1
	}()
	select {
	case <-done:
		return false
	case <-time.After(timeout):
		return true
	}
}


func (kv *ShardKV) printLatestConfig(c master.Config)  {
	gid2Shards := make(map[int][]int)
	for s, gid := range c.Shards {
		if _, ok := gid2Shards[gid]; !ok {
			gid2Shards[gid] = make([]int, 0)
		}
		gid2Shards[gid] = append(gid2Shards[gid], s)
	}
	kv.log.Infof("KVServer %d config: Num=%d", kv.me, c.Num)
	for gid, shards := range gid2Shards {
		kv.log.Infof("%d -> %v", gid, shards)
	}
}

func runFuncName()string{
	pc := make([]uintptr,1)
	runtime.Callers(2,pc)
	f := runtime.FuncForPC(pc[0])
	return f.Name()
}