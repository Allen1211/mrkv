package node

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"

	"mrkv/src/common"
	"mrkv/src/common/labgob"
	"mrkv/src/master"
	"mrkv/src/netw"
	"mrkv/src/node/etc"
	"mrkv/src/replica"
)

type Node struct {
	logger		*logrus.Logger
	mu 			sync.RWMutex

	userConf	etc.NodeConf

	Id 			int
	Host 		string
	Port 		int

	mck			*master.Clerk

	store		replica.Store

	groups    map[int]*Group
	nodeInfos map[int]master.NodeInfo
	nodeEnds  map[int]*netw.ClientEnd
	routeConf master.ConfigV1
	latestConf master.ConfigV1

	KilledC		chan int
}

func (n *Node) Addr() string {
	return fmt.Sprintf("%s:%d", n.Host, n.Port)
}

func MakeNode(userConf etc.NodeConf, masters []*netw.ClientEnd, logLevel string) *Node {
	node := &Node{
		Id: userConf.NodeId,
		Host: userConf.Host,
		Port: userConf.Port,

		mu: sync.RWMutex{},
		userConf: userConf,
		mck: master.MakeClerk(masters),

		groups:    map[int]*Group{},
		nodeInfos: map[int]master.NodeInfo{},
		nodeEnds: map[int]*netw.ClientEnd{},

		KilledC: make(chan int, 10),
	}
	node.logger, _ = common.InitLogger(logLevel, fmt.Sprintf("Node%d", node.Id))

	store, err := replica.MakeLevelStore(fmt.Sprintf("%s/node-%d/meta", userConf.DBPath, node.Id))
	if err != nil {
		node.logger.Fatalf("cannot open leveldb handler: %v", err)
	}
	node.store = store

	node.recover()

	go node.daemon("heartbeater", node.heartbeat, 300*time.Millisecond)

	return node
}

func (n *Node) Kill() {
	for gid, group := range n.groups {
		group.replica.Kill()
		n.logger.Warnf("replica %d was killed", gid)
	}
	n.KilledC <- 1
}

func (n *Node) StartRPCServer() error {
	if err := rpc.RegisterName(fmt.Sprintf("Node-%d", n.Id) , n); err != nil {
		return err
	}
	l, err := net.Listen("tcp", n.Addr())
	if err != nil {
		return err
	}
	go http.Serve(l, nil)
	return nil
}

func (n *Node) daemon(name string, f func(), tick time.Duration) {
	ticker := time.Tick(tick)
	for {
		select {
		case <-n.KilledC:
			n.logger.Warnf("daemon goroutine %s was killed", name)
		case <-ticker:
			f()
		}
	}
}

func (n *Node) recover()  {
	// recover meta data from store

	meta, err := n.loadMetaData()
	if err != nil {
		n.logger.Fatalf("failed to load meta data: %v", err)
	}
	n.latestConf = meta.LatestConf
	saves := meta.Groups
	for _, save := range saves {
		group := &Group {
			Id:        save.Id,
			Status:    save.Status,
			Peer:      save.Peer,
			RaftPeers: save.RaftPeers,
		}
		n.logger.Infof("recover group :%v", *group)
		if save.Status != master.GroupRemoved {
			group.replica = n.doStartReplica(save.Id, save.Peer, save.RaftPeers)
			n.logger.Infof("start group %d", group.Id)
		}
		n.groups[save.Id] = group
	}
	// pull configuration
	n.heartbeat()
}

func (n *Node) heartbeat()  {
	n.mu.RLock()
	groups := map[int]*master.GroupInfo{}
	for gid, group := range n.groups {
		groups[gid] = group.GetGroupInfo()
	}
	n.mu.RUnlock()

	n.logger.Debugf("begin to send heartbeat, current groups: %v", groups)

	reply := n.mck.Heartbeat(n.Id, n.Addr(), groups)

	n.mu.Lock()
	defer n.mu.Unlock()

	configs, nodes := reply.Configs, reply.Nodes

	n.logger.Debugf("received heartbeat reply: %v", configs)

	n.routeConf = reply.LatestConf

	for nodeId, info := range nodes {
		if localInfo, ok := n.nodeInfos[nodeId]; !ok {
			n.nodeInfos[nodeId] = info
			n.createNodeEnd(nodeId)
			n.logger.Infof("found new node: %v", info)
		} else if localInfo.Addr != info.Addr {
			localInfo.Addr = info.Addr
			n.createNodeEnd(nodeId)
			n.logger.Infof("found node %d addr change to %s", nodeId, info.Addr)
		}
	}

	for gid, remoteGroup := range reply.Groups {
		config := reply.Configs[gid]

		if localGroup, ok := n.groups[gid]; !ok || localGroup.Status == master.GroupRemoved {
			if remoteGroup.Status == master.GroupJoined {
				// new group join in
				n.logger.Infof("group %d is newly gain, startNewGroup, remoteGroup=%v", gid, remoteGroup)
				n.groups[gid] = n.startNewGroup(remoteGroup, reply.PrevConfigs[gid], config)
			}
		} else {
			n.logger.Debugf("group %d exists, update..", gid)
			// pass config to replica
			localGroup.UpdateConfig(config)

			if remoteGroup.Status == master.GroupRemoving {
				if localGroup.Status != master.GroupRemoving {
					localGroup.Status = master.GroupRemoving
					n.logger.Infof("group %d status %d => %d", gid, localGroup.Status, remoteGroup.Status)
				}

			} else if remoteGroup.Status == master.GroupRemoved {
				n.logger.Infof("group %d need to shutdown", gid)
				localGroup.Shutdown()
				localGroup.Status = master.GroupRemoved
				n.logger.Infof("group %d successfully shutdown", gid)

			} else if localGroup.Status != remoteGroup.Status {
				n.logger.Infof("group %d status %d => %d", gid, localGroup.Status, remoteGroup.Status)
				localGroup.Status = remoteGroup.Status
			}
		}

		if config.Num > n.latestConf.Num {
			n.latestConf = config
		}
	}
	if err := n.saveMetaData(); err != nil {
		n.logger.Fatalf("faile to save meta data: %v", err)
	}

	n.logger.Debugf("heartbeat finished")
	n.printGroupsInfo()
}

func (n *Node) startNewGroup(remoteGroup master.GroupInfo, prevConfig, currConfig master.ConfigV1) *Group {
	gid := remoteGroup.Id
	ngs := n.routeConf.Groups[gid]

	raftPeers := len(ngs)
	var me int
	for _, ng := range ngs {
		if ng.NodeId == n.Id {
			me = ng.RaftPeer
		}
	}

	r := n.doStartReplica(gid, me, raftPeers)

	// num := n.latestConf.Num
	// if num > 0 {
	// 	prevConfig = n.mck.Query(num-1)
	// 	currConfig = n.mck.Query(num)
	// }

	r.InitConfig(prevConfig, currConfig)

	return MakeGroup(gid, me, raftPeers, remoteGroup.Status, r)
}

func (n *Node) doStartReplica(gid, me, raftPeers int) *replica.ShardKV {
	var err error

	dbPath := fmt.Sprintf("%s/node-%d/replica-%d", n.userConf.DBPath, n.Id, gid)
	var store replica.Store
	if store, err = replica.MakeLevelStore(dbPath); err != nil {
		n.logger.Fatalf("failed to make levelStore at %s : %v", dbPath, err)
		return nil
	}

	logFileName := fmt.Sprintf(n.userConf.Raft.WalDir + "/node%d/logfile%d-%d", n.Id, gid, me)
	logFileCap := n.userConf.Raft.WalCap
	raftDataDir := dbPath

	return replica.StartServer(raftDataDir, logFileName, logFileCap, me, raftPeers, gid, store, n.rpcFuncImpl, n.userConf.Serv.LogLevel)

}

func (n *Node) printGroupsInfo()  {
	for gid, g := range n.groups {
		n.logger.Debugf("gid=%d, status=%d", gid, g.Status)
	}
}

type NodeMetaForSave struct {
	Groups  	[]GroupInfoForSave
	LatestConf	master.ConfigV1
}

type GroupInfoForSave struct {
	Id        int
	Status    master.GroupStatus
	Peer      int
	RaftPeers int
}

func (n *Node) saveMetaData() error {
	saves := make([]GroupInfoForSave, 0)
	for _, g := range n.groups {
		saves = append(saves, GroupInfoForSave {
			Id:     	g.Id,
			Status: 	g.Status,
			Peer:   	g.Peer,
			RaftPeers: 	g.RaftPeers,
		})
	}
	meta := NodeMetaForSave {
		Groups: 	saves,
		LatestConf: n.latestConf,
	}
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	if err := encoder.Encode(meta); err != nil {
		return err
	}
	return n.store.Put(replica.KeyNodeGroup, buf.Bytes())
}

func (n *Node) loadMetaData() (NodeMetaForSave, error) {
	meta := NodeMetaForSave{
		Groups: make([]GroupInfoForSave, 0),
	}

	val, err := n.store.Get(replica.KeyNodeGroup)
	if err != nil && err != leveldb.ErrNotFound {
		return meta, err
	} else if val == nil || len(val) == 0 {
		return meta, nil
	}

	buf := bytes.NewBuffer(val)
	decoder := labgob.NewDecoder(buf)
	if err := decoder.Decode(&meta); err != nil {
		return meta, err
	}
	return meta, nil
}