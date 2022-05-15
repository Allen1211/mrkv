package test

import (
	"bytes"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/pprof"
	"sort"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	master2 "github.com/Allen1211/mrkv/internal/master"
	etc2 "github.com/Allen1211/mrkv/internal/master/etc"
	netw2 "github.com/Allen1211/mrkv/internal/netw"
	node2 "github.com/Allen1211/mrkv/internal/node"
	"github.com/Allen1211/mrkv/internal/node/etc"
	raft2 "github.com/Allen1211/mrkv/internal/raft"
	replica2 "github.com/Allen1211/mrkv/internal/replica"
	client2 "github.com/Allen1211/mrkv/pkg/client"
	common2 "github.com/Allen1211/mrkv/pkg/common"
	labgob2 "github.com/Allen1211/mrkv/pkg/common/labgob"
	utils2 "github.com/Allen1211/mrkv/pkg/common/utils"
)

var masterAddrs = []string{":8000", ":8001", ":8002"}

func init() {
	labgob2.Register(master2.OpBase{})
	labgob2.Register(master2.OpJoinCmd{})
	labgob2.Register(master2.OpLeaveCmd{})
	labgob2.Register(master2.OpMoveCmd{})
	labgob2.Register(master2.OpQueryCmd{})
	labgob2.Register(master2.OpHeartbeatCmd{})
	labgob2.Register(master2.OpShowCmd{})
	labgob2.Register(replica2.Op{})
	labgob2.Register(replica2.CmdBase{})
	labgob2.Register(replica2.KVCmd{})
	labgob2.Register(replica2.ConfCmd{})
	labgob2.Register(replica2.InstallShardCmd{})
	labgob2.Register(replica2.EraseShardCmd{})
	labgob2.Register(replica2.SnapshotCmd{})
	labgob2.Register(replica2.StopWaitingShardCmd{})
	labgob2.Register(replica2.EmptyCmd{})
	labgob2.Register(raft2.InstallSnapshotMsg{})

	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{
		Addr:    ":8989",
		Handler: pprofHandler,
	}
	go server.ListenAndServe()

}

func clearAllData()  {
	utils2.DeleteDir("E:\\MyWorkPlace\\Project\\MultiRaftKV\\data")
	utils2.DeleteDir("E:\\MyWorkPlace\\Project\\MultiRaftKV\\logs")
}

func startMaster(id int) *master2.ShardMaster {
	addrs := masterAddrs
	conf := etc2.MakeDefaultConfig()
	conf.Serv.Me = id
	conf.Serv.Servers = addrs
	conf.Serv.LogLevel = "panic"
	conf.Raft.LogLevel = "panic"

	server := master2.StartServer(conf)
	if server != nil {
		if err := server.StartRPCServer(); err != nil {
			log.Fatalf("Start Raft RPC Server Error: %v", err)
		}
	}

	return server
}

func startMasters(n int) []*master2.ShardMaster {
	addrs := masterAddrs
	masterConfs := make([]etc2.MasterConf, n)
	for i, _ := range masterConfs {
		masterConfs[i] = etc2.MakeDefaultConfig()
		masterConfs[i].Serv.Me = i
		masterConfs[i].Serv.Servers = addrs[:n]
		masterConfs[i].Serv.LogLevel = "panic"
		masterConfs[i].Raft.LogLevel = "panic"
	}
	servers := make([]*master2.ShardMaster, n)
	wg := sync.WaitGroup{}
	for i, conf := range masterConfs {
		wg.Add(1)
		go func(j int, config etc2.MasterConf) {
			server := master2.StartServer(config)
			servers[j] = server
			wg.Done()
			if server != nil {
				if err := server.StartRPCServer(); err != nil {
					log.Fatalf("Start Raft RPC Server Error: %v", err)
				}
			}
		}(i, conf)
	}
	wg.Wait()
	return servers
}

func startNode(id int) *node2.Node {
	masters := make([]*netw2.ClientEnd, len(masterAddrs))
	for i, addr := range masterAddrs {
		masters[i] =  netw2.MakeRPCEnd(fmt.Sprintf("Master%d", i), addr)
	}
	conf := etc.MakeDefaultConfig()
	conf.NodeId = id
	conf.Host = "localhost"
	conf.Port = 8100 + id
	conf.Masters = masterAddrs

	conf.Serv.LogLevel = "panic"
	conf.Raft.LogLevel = "panic"

	nd := node2.MakeNode(conf, masters, conf.Serv.LogLevel)
	go func() {
		if err := nd.StartRPCServer(); err != nil {
			log.Fatalf("Start Raft RPC Server Error: %v", err)
		}
	}()

	return nd
}

func startNodes(n int) []*node2.Node {
	masters := make([]*netw2.ClientEnd, len(masterAddrs))
	for i, addr := range masterAddrs {
		masters[i] =  netw2.MakeRPCEnd(fmt.Sprintf("Master%d", i), addr)
	}
	nodes := make([]*node2.Node, n)
	for i := 1; i <= n; i++ {
		conf := etc.MakeDefaultConfig()
		conf.NodeId = i
		conf.Host = "localhost"
		conf.Port = 8100 + i
		conf.Masters = masterAddrs

		conf.Serv.LogLevel = "panic"
		conf.Raft.LogLevel = "panic"

		nd := node2.MakeNode(conf, masters, conf.Serv.LogLevel)
		if err := nd.StartRPCServer(); err != nil {
			log.Fatalf("Start Raft RPC Server Error: %v", err)
		}
		nodes[i-1] = nd
	}
	return nodes

}

func stopMaster(masters ...*master2.ShardMaster)  {
	for _, server := range masters {
		server.Kill()
	}
}

func stopNode(nodes ...*node2.Node)  {
	for _, server := range nodes {
		if !server.Killed() {
			server.Kill()
		}
	}
}

func makeMasterClient() *master2.Clerk {
	masters := make([]*netw2.ClientEnd, len(masterAddrs))
	for i, addr := range masterAddrs {
		masters[i] =  netw2.MakeRPCEnd(fmt.Sprintf("Master%d", i), addr)
	}
	return master2.MakeClerk(masters)
}

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}


func TestMasterStartUp(t *testing.T) {
	clearAllData()
	servers := startMasters(3)
	time.Sleep(3*time.Second)
	stopMaster(servers...)

	clearAllData()
	servers = startMasters(2)
	time.Sleep(2*time.Second)
	stopMaster(servers...)

	clearAllData()
	servers = startMasters(1)
	if servers[0] != nil {
		t.FailNow()
	}
}

func TestNodeStartUp(t *testing.T) {
	clearAllData()

	servers := startMasters(3)
	time.Sleep(1*time.Second)

	nodes := startNodes(3)
	time.Sleep(5*time.Second)

	stopNode(nodes...)
	stopMaster(servers...)
}

func TestShowMaster(t *testing.T) {
	servers := startMasters(3)
	time.Sleep(5*time.Second)
	//
	// nodes := startNodes(3)
	// time.Sleep(3*time.Second)

	for i := 0; i < 5; i++ {
		cli := client2.MakeMrKVClient(masterAddrs)
		for j := 0; j < 10; j++ {
			showMasterRes := cli.ShowMaster()
			if len(showMasterRes) != 3 {
				t.Fatalf("len of showMasterRes %d != 3", len(showMasterRes))
				t.FailNow()
			}
			for i, res := range showMasterRes {
				if res.Status != "Normal" {
					t.Fatalf("master %d status is %s", i, res.Status)
					t.FailNow()
				}
			}
		}
	}

	servers[0].Kill()
	time.Sleep(5 * time.Second)

	cli := client2.MakeMrKVClient(masterAddrs)
	for j := 0; j < 10; j++ {
		showMasterRes := cli.ShowMaster()
		if len(showMasterRes) != 3 {
			t.Fatalf("len of showMasterRes %d != 3", len(showMasterRes))
			t.FailNow()
		}
		for i, res := range showMasterRes {
			if i == 0 {
				if res.Status == "Normal" {
					t.Fatalf("master %d status is %s", i, res.Status)
					t.FailNow()
				}
			} else {
				if res.Status != "Normal" {
					t.Fatalf("master %d status is %s", i, res.Status)
					t.FailNow()
				}
			}
		}
	}

	servers[1].Kill()
	servers[2].Kill()
	time.Sleep(2 * time.Second)
}

func TestShowNode(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(3)
	time.Sleep(3*time.Second)
	cli := client2.MakeMrKVClient(masterAddrs)

	showNodes, err := cli.ShowNodes([]int{})
	if err != common2.OK {
		t.Fatalf("err is %s", err)
		t.FailNow()
	}
	if len(showNodes) != 3 {
		t.Fatalf("err is %s", err)
		t.FailNow()
	}
	sort.Slice(showNodes, func(i, j int) bool {
		return showNodes[i].Id < showNodes[j].Id
	})
	for _, n := range showNodes {
		if n.Found && n.Status != master2.NodeNormal.String() {
			t.FailNow()
		}
	}

	nodes[0].Kill()
	time.Sleep(6*time.Second)

	showNodes, err = cli.ShowNodes([]int{1})
	if err != common2.OK {
		t.Fatalf("err is %s", err)
		t.FailNow()
	}
	if len(showNodes) != 1 {
		t.Fatalf("err is %s", err)
		t.FailNow()
	}
	n := showNodes[0]
	if !n.Found || n.Id != 1 || n.Status != master2.NodeDisconnect.String() {
		t.Fatalf("node %d status is %s", n.Id, n.Status)
		t.FailNow()
	}

	nodes[1].Kill()
	nodes[2].Kill()

	stopMaster(masters...)
}

func TestShowGroupShowShards(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(4)
	time.Sleep(3*time.Second)
	cli := client2.MakeMrKVClient(masterAddrs)
	cli.Join(100, []int{1, 2, 3})
	cli.Join(200, []int{1, 3, 4})
	time.Sleep(10*time.Second)

	groups, err := cli.ShowGroups([]int{})
	if err != common2.OK {
		t.Fatalf("show gruops error: %v", err)
		t.FailNow()
	}
	if len(groups) != 2 {
		t.Fatalf("show groups len %d != 2", len(groups))
		t.FailNow()
	}
	for _, g := range groups {
		if g.Id != 100 && g.Id != 200 {
			t.FailNow()
		}
		if !g.Found || len(g.ByNode) != 3 {
			t.FailNow()
		}
		if g.ShardCnt != master2.NShards/ 2 {
			t.Fatalf("shard cnt %d != 16", g.ShardCnt)
			t.FailNow()
		}
	}

	shards, err := cli.ShowShards([]int{100, 200})
	if err != common2.OK {
		t.FailNow()
	}
	if len(shards) != master2.NShards {
		t.Fatalf("len of shrads not eq to 32")
		t.FailNow()
	}
	for _, shard := range shards {
		if shard.Status != master2.SERVING || (shard.Gid != 100 && shard.Gid != 200) {
			t.Fatalf("%v", shard)
			t.FailNow()
		}
	}

	stopMaster(masters...)
	stopNode(nodes...)
}

func TestJoinLeave(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(4)
	time.Sleep(3*time.Second)
	cli := client2.MakeMrKVClient(masterAddrs)

	cli.Join(100, []int{1, 2, 3})
	cli.Join(200, []int{1, 3, 4})
	time.Sleep(6*time.Second)

	groups, err := cli.ShowGroups([]int{})
	if err != common2.OK {
		t.Fatalf("show gruops error: %v", err)
		t.FailNow()
	}
	if len(groups) != 2 {
		t.Fatalf("show groups len %d != 2", len(groups))
		t.FailNow()
	}
	for _, g := range groups {
		if g.Id != 100 && g.Id != 200 {
			t.FailNow()
		}
		if !g.Found || len(g.ByNode) != 3 {
			t.FailNow()
		}
		if g.ShardCnt != master2.NShards/ 2 {
			t.Fatalf("shard cnt %d != 16", g.ShardCnt)
			t.FailNow()
		}
	}

	cli.Leave(100)
	time.Sleep(5*time.Second)

	groups, err = cli.ShowGroups([]int{})
	if err != common2.OK {
		t.Fatalf("show gruops error: %v", err)
		t.FailNow()
	}
	if len(groups) != 1 {
		t.Fatalf("show groups len %d != 1", len(groups))
		t.FailNow()
	}
	if groups[0].Id != 200 {
		t.FailNow()
	}
	for _, g := range groups {
		if !g.Found || len(g.ByNode) != 3 {
			t.FailNow()
		}
		if g.ShardCnt != master2.NShards {
			t.Fatalf("shard cnt != 32")
			t.FailNow()
		}
	}

	cli.Leave(200)
	time.Sleep(8*time.Second)

	groups, err = cli.ShowGroups([]int{})
	if err != common2.OK {
		t.Fatalf("show gruops error: %v", err)
		t.FailNow()
	}
	if len(groups) != 0 {
		t.Fatalf("show groups len %d != 0", len(groups))
		t.FailNow()
	}

	stopMaster(masters...)
	stopNode(nodes...)
}

func TestLeaderBalancer(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(3)
	time.Sleep(3*time.Second)
	cli := client2.MakeMrKVClient(masterAddrs)

	cli.Join(100, []int{1, 2, 3})
	cli.Join(200, []int{1, 2, 3})
	cli.Join(300, []int{1, 2, 3})

	time.Sleep(10*time.Second)

	showNodes, err := cli.ShowNodes([]int{})
	if err != common2.OK {
		t.FailNow()
	}
	for _, nd := range showNodes {
		cnt := 0
		for _, gid := range nd.Groups {
			if nd.IsLeader[gid] {
				cnt++
			}
		}
		if cnt != 1 {
			t.Fatalf("node %d has %d leader", nd.Id, cnt)
			t.FailNow()
		}
	}

	stopMaster(masters...)
	stopNode(nodes...)
}

func TestCURD(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(4)
	time.Sleep(3*time.Second)
	cli := client2.MakeMrKVClient(masterAddrs)

	cli.Join(100, []int{1, 2, 3})
	cli.Join(200, []int{1, 3, 4})
	time.Sleep(8*time.Second)

	mp := make(map[string]string)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d", i)
		val := randstring(10)
		mp[key] = val
		reply := cli.Put(key, []byte(val))
		if reply.Err != common2.OK {
			t.FailNow()
		}
	}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d", i)
		reply := cli.Get(key)
		if reply.Err != common2.OK || string(reply.Value) != mp[key]{
			t.FailNow()
		}
	}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d", i)
		val := randstring(3)
		mp[key] += val
		cli.Append(key, []byte(val))
	}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d", i)
		reply := cli.Get(key)
		if reply.Err != common2.OK || string(reply.Value) != mp[key]{
			t.FailNow()
		}
	}
	for i := 0; i < 1000; i += 2 {
		key := fmt.Sprintf("%d", i)
		delete(mp, key)
		cli.Delete(key)
	}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%d", i)
		val, ok := mp[key]
		reply := cli.Get(key)
		if reply.Err != common2.OK && reply.Err != common2.ErrNoKey {
			t.FailNow()
		}
		if ok && (reply.Err != common2.OK || string(reply.Value) != val) || !ok && reply.Err == common2.OK {
			t.FailNow()
		}
	}

	stopNode(nodes...)
	stopMaster(masters...)
}

func TestConcurrentCURD(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(4)
	time.Sleep(3*time.Second)
	cli := client2.MakeMrKVClient(masterAddrs)

	cli.Join(100, []int{1, 2, 3})
	cli.Join(200, []int{1, 3, 4})
	time.Sleep(8*time.Second)

	m := 1000

	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(k int) {
			cli := client2.MakeMrKVClient(masterAddrs)
			for j := m*k; j < m; j++ {
				key := fmt.Sprintf("%d", m)
				cli.Put(key, []byte(randstring(10)))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(k int) {
			cli := client2.MakeMrKVClient(masterAddrs)
			for j := m*k; j < m; j++ {
				key := fmt.Sprintf("%d", m)
				cli.Append(key, []byte(randstring(5)))
			}
			wg.Done()
		}(i)
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(k int) {
			cli := client2.MakeMrKVClient(masterAddrs)
			for j := m*k; j < m; j++ {
				key := fmt.Sprintf("%d", m)
				cli.Get(key)
			}
			wg.Done()
		}(i)
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(k int) {
			cli := client2.MakeMrKVClient(masterAddrs)
			for j := m*k; j < m; j++ {
				key := fmt.Sprintf("%d", m)
				cli.Delete(key)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	stopNode(nodes...)
	stopMaster(masters...)
}

func TestJoinLeaveWhileConcurrentGetPut(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(5)
	time.Sleep(3*time.Second)

	cli := makeMasterClient()

	cli.Join(map[int][]int{
		300: {1, 2, 4},
	})
	// fmt.Println(3)
	time.Sleep(5*time.Second)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		cli := client2.MakeMrKVClient(masterAddrs)

		m := 100
		mp := make(map[string]string)
		for i := 0; i < m; i++ {
			key := fmt.Sprintf("%d", i)
			val := randstring(10)
			mp[key] = val
			reply := cli.Put(key, []byte(val))
			if reply.Err != common2.OK {
				t.FailNow()
			}
			time.Sleep(50*time.Millisecond)
		}
		for i := 0; i < m; i++ {
			key := fmt.Sprintf("%d", i)
			reply := cli.Get(key)
			if reply.Err != common2.OK || string(reply.Value) != mp[key]{
				t.FailNow()
			}
			time.Sleep(50*time.Millisecond)
		}
		for i := 0; i < m; i++ {
			key := fmt.Sprintf("%d", i)
			val := randstring(3)
			mp[key] += val
			cli.Append(key, []byte(val))
			time.Sleep(50*time.Millisecond)
		}
		for i := 0; i < m; i++ {
			key := fmt.Sprintf("%d", i)
			reply := cli.Get(key)
			if reply.Err != common2.OK || string(reply.Value) != mp[key]{
				t.FailNow()
			}
			time.Sleep(50*time.Millisecond)
		}
		for i := 0; i < m; i += 2 {
			key := fmt.Sprintf("%d", i)
			delete(mp, key)
			cli.Delete(key)
			time.Sleep(50*time.Millisecond)
		}
		for i := 0; i < m; i++ {
			key := fmt.Sprintf("%d", i)
			val, ok := mp[key]
			reply := cli.Get(key)
			if reply.Err != common2.OK && reply.Err != common2.ErrNoKey {
				t.FailNow()
			}
			if ok && (reply.Err != common2.OK || string(reply.Value) != val) || !ok && reply.Err == common2.OK {
				t.FailNow()
			}
			time.Sleep(50*time.Millisecond)
		}
		wg.Done()
	}()

	for i := 0; i < 1; i++ {
		// fmt.Println("###################")

		for cli.Join(map[int][]int{100: {1, 4, 5}, 200: {2, 3, 4},}) != common2.OK {
			// t.Errorf("join: err")
			time.Sleep(1*time.Second)
		}
		// fmt.Println(123)
		time.Sleep(5*time.Second)

		for cli.Leave([]int{200}) != common2.OK {
			// t.Errorf("leave: err")
			time.Sleep(1*time.Second)
		}
		// fmt.Println(13)
		time.Sleep(5*time.Second)

		for  cli.Leave([]int{100}) != common2.OK {
			// t.Errorf("leave: err")
			time.Sleep(1*time.Second)
		}
		// fmt.Println(3)
		time.Sleep(5*time.Second)

		for cli.Join(map[int][]int{200: {1, 2, 3}}) != common2.OK {
			// t.Errorf("join: err")
			time.Sleep(1*time.Second)
		}
		// fmt.Println(23)
		time.Sleep(5*time.Second)

		for cli.Join(map[int][]int{100: {2, 3, 5}}) != common2.OK {
			// t.Errorf("join: err")
			time.Sleep(1*time.Second)
		}
		// fmt.Println(123)
		time.Sleep(5*time.Second) // 83

		for cli.Leave([]int{200}) != common2.OK {
			// t.Errorf("leave: err")
			time.Sleep(1*time.Second)
		}
		// fmt.Println(13)
		time.Sleep(5*time.Second)

		for cli.Leave([]int{100}) != common2.OK {
			// t.Errorf("leave: err")
			time.Sleep(1*time.Second)
		}
		// fmt.Println(3)
		time.Sleep(5*time.Second)
	}
	wg.Wait()

	stopNode(nodes...)
	stopMaster(masters...)
}

func TestRestart(t *testing.T) {
	clearAllData()
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(4)
	time.Sleep(3*time.Second)

	cli := client2.MakeMrKVClient(masterAddrs)
	cli.Join(100, []int{1, 2, 3})
	cli.Join(200, []int{1, 2, 3})
	time.Sleep(6*time.Second)

	stopMaster(masters[0], masters[1])
	time.Sleep(2*time.Second)

	masters[0] = startMaster(0)
	masters[1] = startMaster(1)
	time.Sleep(3*time.Second)
	// cli = client.MakeMrKVClient(masterAddrs)

	groups, err := cli.ShowGroups([]int{})
	if err != common2.OK {
		t.Fatalf("show gruops error: %v", err)
		t.FailNow()
	}
	if len(groups) != 2 {
		t.Fatalf("show groups len %d != 2", len(groups))
		t.FailNow()
	}
	for _, g := range groups {
		if g.Id != 100 && g.Id != 200 {
			t.FailNow()
		}
		if !g.Found || len(g.ByNode) != 3 {
			t.FailNow()
		}
		if g.ShardCnt != master2.NShards/ 2 {
			t.Fatalf("shard cnt %d != 16", g.ShardCnt)
			t.FailNow()
		}
	}

	m := 1000
	mp := make(map[string]string)
	for i := 0; i < m; i++ {
		key := fmt.Sprintf("%d", i)
		val := randstring(10)
		mp[key] = val
		reply := cli.Put(key, []byte(val))
		if reply.Err != common2.OK {
			t.FailNow()
		}
	}
	stopNode(nodes[0], nodes[1], nodes[2])
	time.Sleep(2*time.Second)

	nodes[0] = startNode(1)
	nodes[1] = startNode(2)
	nodes[2] = startNode(3)
	// nodes = startNodes(4)
	time.Sleep(4*time.Second)

	// cli = client.MakeMrKVClient(masterAddrs)
	for i := 0; i < m; i++ {
		key := fmt.Sprintf("%d", i)
		reply := cli.Get(key)
		if reply.Err != common2.OK || string(reply.Value) != mp[key]{
			t.FailNow()
		}
	}

	stopMaster(masters...)
	stopNode(nodes...)
}

/*
func TestConsoleClient(t *testing.T) {
	masters := startMasters(3)
	time.Sleep(1*time.Second)
	nodes := startNodes(4)
	time.Sleep(3*time.Second)

	buf := bytes.NewBuffer(make([]byte, 0))
	in := bufio.NewScanner(buf)
	cli := client.MakeConsoleClient(etc.ClientConf {
		Masters: masterAddrs,
	}, in, nil)
	go cli.Start()

	_, _ = buf.WriteString("help\n")
	_, _ = buf.WriteString("show master\n")
	_, _ = buf.WriteString("show node\n")
	_, _ = buf.WriteString("show group\n")
	_, _ = buf.WriteString("show shard 100\n")
	_, _ = buf.WriteString("get 1\n")
	_, _ = buf.WriteString("put 1 x\n")
	_, _ = buf.WriteString("append 1 y\n")
	_, _ = buf.WriteString("del 1\n")
	_, _ = buf.WriteString("trans_leader 100 1\n")
	_, _ = buf.WriteString("leave 100\n")
	time.Sleep(5*time.Second)
	_, _ = buf.WriteString("join 100 1 2 3\n")
	time.Sleep(5*time.Second)
	_, _ = buf.WriteString("show group\n")
	_, _ = buf.WriteString("sdfsdf\n")
	_, _ = buf.WriteString("\n")

	stopMaster(masters...)
	stopNode(nodes...)
}*/

func TestGateWay(t *testing.T) {
	args := &master2.ShowMasterArgs{}
	data := utils2.MsgpEncode(args)
	req, err := http.NewRequest("POST", "http://127.0.0.1:8000/", bytes.NewReader(data))
	if err != nil {
		log.Fatal("failed to create request: ", err)
		return
	}

	h := req.Header
	h.Set("X-RPCX-MesssageType", "0")
	h.Set("X-RPCX-SerializeType", "5")
	h.Set("X-RPCX-ServicePath", "Master0")
	h.Set("X-RPCX-ServiceMethod", "ShowMaster")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("failed to call: ", err)
	}
	defer res.Body.Close()

	// handle http response
	replyData, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatal("failed to read response: ", err)
	}

	reply := &master2.ShowMasterReply{}
	utils2.MsgpDecode(replyData, reply)

	fmt.Printf("%+v", reply)
}