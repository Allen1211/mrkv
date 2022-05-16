package master

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/allen1211/mrkv/internal/master/etc"
	"github.com/allen1211/mrkv/internal/netw"
	"github.com/allen1211/mrkv/internal/raft"
	"github.com/allen1211/mrkv/pkg/common"
	"github.com/allen1211/mrkv/pkg/common/utils"
)

var (
	opsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "mrkv_master",
		Name:      "cnt",
		Help:      "The total number of processed events",
	})
)

type ShardMaster struct {
	mu      sync.RWMutex
	me      int
	servers []*netw.ClientEnd
	listener net.Listener
	rpcServ  *netw.RpcxServer
	persister *raft.DiskPersister
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	nodes		map[int]*Node

	configs     []common.ConfigV1 // indexed by config num
	routeConfig common.ConfigV1

	opApplied   map[int]chan interface{}
	ckMaxSeq    map[int64]int64
	lastApplied int
	appliedCond *sync.Cond

	KilledC chan int
	killed  int32

	log		*logrus.Logger

	metricAddr string
}

type Node struct {
	Id       int
	Addr     string
	Groups   map[int]*common.GroupInfo
	Status   common.NodeStatus
	LastBeat time.Time
	MetricAddr string
}

func (sm *ShardMaster) applyer() {
	for {
		select {
		case msg := <-sm.applyCh:
			if !msg.CommandValid {
				continue
			}
			wrap := utils.DecodeCmdWrap(msg.Command)

			sm.mu.Lock()

			if wrap.Type != common.CmdTypeSnap {
				if msg.CommandIndex <= sm.lastApplied {
					sm.log.Debugf("ShardMaster %d Applyer msg idx(%d) less eq to lastApplied(%d)", sm.me, msg.CommandIndex, sm.lastApplied)
					sm.mu.Unlock()
					continue
				}
			}

			switch wrap.Type {
			case common.CmdTypeSnap:
				snapCmd := raft.InstallSnapshotMsg{}
				utils.MsgpDecode(wrap.Body, &snapCmd)

				sm.log.Infof("recieved install snapshot msg, lastIncludedIdx=%d", snapCmd.LastIncludedIdx)
				if !sm.rf.CondInstallSnapshot(snapCmd.LastIncludedTerm, snapCmd.LastIncludedIdx, snapCmd.LastIncludedEndLSN, snapCmd.Data) {
					sm.mu.Unlock()
					continue
				}

				if snapCmd.LastIncludedIdx <= sm.lastApplied {
					sm.mu.Unlock()
					continue
				}
				if err := sm.applySnapshot(snapCmd.Data); err != nil {
					sm.log.Errorf("failed to apply snapshot: %v", err)
				}
				sm.log.Infof("applied snapshot, lastIncludedIdx=%d", snapCmd.LastIncludedIdx)

				sm.lastApplied = snapCmd.LastIncludedIdx
				sm.appliedCond.Broadcast()
				sm.mu.Unlock()
				continue

			case common.CmdTypeEmpty:
				sm.lastApplied = msg.CommandIndex
				sm.appliedCond.Broadcast()
				sm.mu.Unlock()
				continue
			}

			var reply interface{}
			var err error

			switch wrap.Type {
			case common.CmdTypeQuery:
				op := common.OpQueryCmd{}
				utils.MsgpDecode(wrap.Body, &op)
				reply, err = sm.executeQuery(&op.Args)
			case common.CmdTypeShow:
				op := common.OpShowCmd{}
				utils.MsgpDecode(wrap.Body, &op)
				reply, err = sm.executeShow(&op.Args)
			case common.CmdTypeHeartbeat:
				op := common.OpHeartbeatCmd{}
				utils.MsgpDecode(wrap.Body, &op)
				if sm.isDuplicateAndSet(op.Cid, op.Seq) {
					break
				}
				reply, err = sm.executeHeartbeat(&op.Args)
			case common.CmdTypeJoin:
				op := common.OpJoinCmd{}
				utils.MsgpDecode(wrap.Body, &op)
				if sm.isDuplicateAndSet(op.Cid, op.Seq) {
					break
				}
				reply, err = sm.executeJoin(&op.Args)
			case common.CmdTypeLeave:
				op := common.OpLeaveCmd{}
				utils.MsgpDecode(wrap.Body, &op)
				if sm.isDuplicateAndSet(op.Cid, op.Seq) {
					break
				}
				reply, err = sm.executeLeave(&op.Args)
			case common.CmdTypeMove:
				op := common.OpMoveCmd{}
				utils.MsgpDecode(wrap.Body, &op)
				if sm.isDuplicateAndSet(op.Cid, op.Seq) {
					break
				}
				reply, err = sm.executeMove(&op.Args)
			default:
				panic("unreconized op type")
			}

			sm.log.Debugf("ShardMaster %d Applyer apply op %d, res is %v, err is %v", sm.me, msg.CommandIndex, reply, err)

			sm.lastApplied = msg.CommandIndex
			sm.appliedCond.Broadcast()

			sm.mu.Unlock()
			if term, isLeader := sm.rf.GetState(); !isLeader || term != msg.CommandTerm {
				continue
			}
			sm.mu.Lock()
			waitC := sm.getWaitCh(msg.CommandIndex)
			sm.mu.Unlock()

			sm.log.Debugf("ShardMaster %d Applyer ready to send msg %v to wait channel", sm.me, msg.CommandIndex)
			select {
			case waitC <- reply:
				sm.log.Debugf("ShardMaster %d Applyer send msg %v to wait channel", sm.me, msg.CommandIndex)
			default:
			}

		case <-sm.KilledC:
			sm.log.Infof("ShardMaster %d has been killed, stop applyer loop", sm.me)
			return
		}
	}
}

func (sm *ShardMaster) raftStart(opType uint8, opBody []byte) (res interface{}, err common.Err) {
	wrap := utils.EncodeCmdWrap(opType, opBody)

	var idx, term int
	var isLeader bool

	if idx, term, isLeader = sm.rf.Start(wrap); !isLeader {
		return nil, common.ErrWrongLeader
	}
	sm.log.Debugf("ShardMaster %d call raft.start res: idx=%d term=%d isLeader=%v", sm.me, idx, term, isLeader)
	//	fmt.Printf("op: %v", op)

	sm.mu.Lock()
	waitC := sm.getWaitCh(idx)
	sm.mu.Unlock()

	defer func() {
		go sm.delWaitChLock(idx)
	}()

	// wait for being applied
	select {
	case opRes := <-waitC:
		// sm.log.Debugf("ShardMaster %d receive res %v from waiting channel %v", sm.me, opRes.res, opRes.idx)
		return opRes, common.OK

	case <-time.After(time.Second * 1):
		sm.log.Warnf("ShardMaster %d op at idx %d has not commited after 1 secs", sm.me, idx)
		return nil, common.ErrFailed
	}
}

func (sm *ShardMaster) Heartbeat(ctx context.Context, args *common.HeartbeatArgs, reply *common.HeartbeatReply) (e error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}

	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = common.ErrWrongLeader
		reply.WrongLeader = true
		reply.Configs = map[int]common.ConfigV1{}
		reply.Nodes = map[int]common.NodeInfo{}
		return
	}

	sm.log.Debugf("Received Heartbeat from node %d", args.NodeId)

	op := common.OpHeartbeatCmd{
		OpBase: common.OpBase{
			Type: common.OpHeartbeat,
			Cid:  args.Cid,
			Seq:  args.Seq,
		},
		Args: *args,
	}

	res, err := sm.raftStart(common.CmdTypeHeartbeat, utils.MsgpEncode(&op))

	if err != common.OK {
		reply.Err = common.Err(err)
		reply.WrongLeader = err == common.ErrWrongLeader
		reply.Configs = map[int]common.ConfigV1{}
		reply.Nodes = map[int]common.NodeInfo{}
	} else {
		reply.Err = common.OK
		r, _ := res.(*common.HeartbeatReply)
		if r != nil {
			*reply = *r
		} else {
			return
		}
	}
	// sm.log.Debugf("ShardMaster %d heartbeat command reply: %v", sm.me, *reply)

	return
}

func (sm *ShardMaster) Join(ctx context.Context, args *common.JoinArgs, reply *common.JoinReply) (e error)  {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}

	op := common.OpJoinCmd{
		OpBase: common.OpBase{
			Type: common.OpJoin,
			Cid:  args.Cid,
			Seq:  args.Seq,
		},
		Args: *args,
	}
	sm.log.Infof("ShardMaster %d receive join command args: %v", sm.me, args)

	for gid, nodeIds := range args.Nodes {
		for _, nodeId := range nodeIds {
			if _, ok := sm.nodes[nodeId]; !ok {
				sm.log.Warnf("cannot join group %d to node %d, node not registered", gid, nodeId)
				reply.Err = common.ErrNodeNotRegister
				return
			}
		}
	}

	_, err := sm.raftStart(common.CmdTypeJoin, utils.MsgpEncode(&op))
	reply.Err = common.Err(err)
	reply.WrongLeader = err == common.ErrWrongLeader

	sm.log.Infof("ShardMaster %d join command reply: %v", sm.me, *reply)
	sm.printLatestConfig(true)

	return
}

func (sm *ShardMaster) Leave(ctx context.Context, args *common.LeaveArgs, reply *common.LeaveReply) (e error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}

	op := common.OpLeaveCmd{
		OpBase: common.OpBase{
			Type: common.OpLeave,
			Cid:  args.Cid,
			Seq:  args.Seq,
		},
		Args: *args,
	}
	sm.log.Infof("ShardMaster %d receive leave command args: %v", sm.me, args)

	res, err := sm.raftStart(common.CmdTypeLeave, utils.MsgpEncode(&op))
	if err != common.OK {
		reply.Err = common.Err(err)
		reply.WrongLeader = err == common.ErrWrongLeader
	} else {
		reply.Err = common.OK
		r, _ := res.(*common.LeaveReply)
		if r != nil {
			*reply = *r
		} else {
			return
		}
	}

	sm.log.Infof("ShardMaster %d leave command reply: %v", sm.me, *reply)
	sm.printLatestConfig(true)

	return
}

func (sm *ShardMaster) Move(ctx context.Context, args *common.MoveArgs, reply *common.MoveReply) (e error)  {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}

	op := common.OpMoveCmd{
		OpBase: common.OpBase{
			Type: common.OpMove,
			Cid:  args.Cid,
			Seq:  args.Seq,
		},
		Args: *args,
	}
	sm.log.Infof("ShardMaster %d receive move command args: %v", sm.me, args)

	_, err := sm.raftStart(common.CmdTypeMove, utils.MsgpEncode(&op))
	reply.Err = common.Err(err)
	reply.WrongLeader = err == common.ErrWrongLeader

	sm.log.Infof("ShardMaster %d move command reply: %v", sm.me, *reply)
	sm.printLatestConfig(true)

	return
}

func (sm *ShardMaster) Query(ctx context.Context, args *common.QueryArgs, reply *common.QueryReply) (e error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}

	op := common.OpQueryCmd{
		OpBase: common.OpBase{
			Type: common.OpQuery,
			Cid:  args.Cid,
			Seq:  args.Seq,
		},
		Args: *args,
	}
	sm.log.Debugf("ShardMaster %d receive query command args: %v", sm.me, *args)

	sm.mu.RLock()
	if args.Num >= 0 && args.Num < len(sm.configs) {
		reply.Config = sm.configs[args.Num]
		sm.mu.RUnlock()
		return
	}
	sm.mu.RUnlock()

	if ok, readIdx := sm.rf.ReadIndex(); ok {
		sm.log.Debugf("ShardMaster %d query ReadIndex %d start to wait", sm.me, readIdx)
		sm.waitAppliedTo(readIdx)
		sm.log.Debugf("ShardMaster %d query ReadIndex %d success", sm.me, readIdx)

		if sm.Killed() {
			reply.Err = common.ErrNodeClosed
			return
		}

		sm.mu.RLock()
		reply.Config = sm.configs[len(sm.configs)-1]
		sm.mu.RUnlock()
		return
	}

	res, err := sm.raftStart(common.CmdTypeQuery, utils.MsgpEncode(&op))
	if err != common.OK {
		reply.Err = common.Err(err)
		reply.WrongLeader = err == common.ErrWrongLeader
	} else {
		reply.Err = common.OK
		r, _ := res.(*common.QueryReply)
		reply.Config = r.Config
	}
	sm.log.Debugf("ShardMaster %d query command reply: %v", sm.me, *reply)
	// sm.printLatestConfig(true)

	return
}

func (sm *ShardMaster) Show(ctx context.Context, args *common.ShowArgs, reply *common.ShowReply) (e error) {
	if sm.Killed() {
		return errors.New(string(common.ErrNodeClosed))
	}

	op := common.OpShowCmd{
		OpBase: common.OpBase{
			Type: common.OpShow,
		},
		Args: *args,
	}
	sm.log.Debugf("ShardMaster %d receive show command args: %v", sm.me, *args)
	if ok, readIdx := sm.rf.ReadIndex(); ok {
		sm.log.Debugf("ShardMaster %d show ReadIndex %d start to wait", sm.me, readIdx)
		sm.waitAppliedTo(readIdx)
		sm.log.Debugf("ShardMaster %d show ReadIndex %d success", sm.me, readIdx)

		if sm.Killed() {
			reply.Err = common.ErrNodeClosed
			return
		}

		sm.mu.RLock()
		r, e := sm.executeShow(args)
		if e != nil {
			return e
		}
		*reply = *r
		sm.mu.RUnlock()
		return nil
	}
	res, err := sm.raftStart(common.CmdTypeShow, utils.MsgpEncode(&op))
	if err != common.OK {
		reply.Err = common.Err(err)
	} else if res == nil {
		reply.Err = common.ErrFailed
	} else {
		reply.Err = common.OK
		r, _ := res.(*common.ShowReply)
		*reply = *r
	}
	sm.log.Debugf("ShardMaster %d show command reply: %v", sm.me, *reply)
	return
}

func (sm *ShardMaster) ShowMaster(ctx context.Context, args *common.ShowMasterArgs, reply *common.ShowMasterReply) (e error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.Killed() {
		reply.Id = sm.me
		reply.Addr = sm.servers[sm.me].Addr
		reply.Status = "Disconnected"
		return
	}

	_, reply.IsLeader = sm.rf.GetState()
	reply.Id = sm.me
	reply.Addr = sm.servers[sm.me].Addr
	reply.LatestConfNum = sm.getLatestConfig().Num
	reply.Size = int64(sm.persister.RaftStateSize() + sm.persister.SnapshotSize())
	reply.Status = "Normal"
	reply.MetricAddr = sm.metricAddr
	return
}

func (sm *ShardMaster) readLatestConfig() (*common.ConfigV1, common.Err) {

	sm.mu.Unlock()
	if ok, readIdx := sm.rf.ReadIndex(); ok {
		sm.log.Debugf("ShardMaster %d query ReadIndex %d start to wait", sm.me, readIdx)
		sm.waitAppliedTo(readIdx)
		sm.log.Debugf("ShardMaster %d query ReadIndex %d success", sm.me, readIdx)

		sm.mu.Lock()

		return &sm.configs[len(sm.configs)-1], common.OK
	}
	op := common.OpQueryCmd{
		OpBase: common.OpBase{
			Type: common.OpQuery,
		},
		Args: common.QueryArgs{
			Num: -1,
		},
	}
	res, err := sm.raftStart(common.CmdTypeQuery, utils.MsgpEncode(&op))

	defer sm.mu.Lock()
	if err != common.OK {
		return nil, common.Err(err)
	} else {
		r, _ := res.(*common.QueryReply)
		return &r.Config, common.OK
	}
}

func (sm *ShardMaster) executeHeartbeat(args *common.HeartbeatArgs) (reply *common.HeartbeatReply, err error) {
	reply = new(common.HeartbeatReply)

	reply.Configs, reply.PrevConfigs ,reply.Nodes, reply.Groups = map[int]common.ConfigV1{}, map[int]common.ConfigV1{}, map[int]common.NodeInfo{}, map[int]common.GroupInfo{}

	// handle node meta data change
	var node *Node
	if val, ok := sm.nodes[args.NodeId]; !ok {
		// new node join in
		node = &Node{
			Id: args.NodeId,
			Addr: args.Addr,
			Groups: args.Groups,
			MetricAddr: args.MetricAddr,
		}
		sm.nodes[args.NodeId] = node

	} else {
		node = val
		node.Addr = args.Addr
		node.MetricAddr = args.MetricAddr
		// node.Groups = args.Groups

		for gid, argGroup := range args.Groups {
			var localGroup *common.GroupInfo
			if localGroup, ok = node.Groups[gid]; !ok {
					if argGroup.Status != common.GroupRemoved {
						node.Groups[gid] = &common.GroupInfo{
							Id: 		gid,
							ConfNum: 	argGroup.ConfNum,
							IsLeader:   argGroup.IsLeader,
							Shards:     argGroup.Shards,
							Size:       argGroup.Size,
							Status:     argGroup.Status,
						}
						localGroup = node.Groups[gid]
					} else {
						continue
					}
			} else if argGroup.ConfNum >= localGroup.ConfNum {
				localGroup.ConfNum = argGroup.ConfNum
				localGroup.Peer = argGroup.Peer
				localGroup.IsLeader = argGroup.IsLeader
				localGroup.Shards = argGroup.Shards
				localGroup.Size = argGroup.Size
			}
			// handle shard meta data change
			outer:
			switch localGroup.Status {
			case common.GroupJoined:
				if argGroup.Status != common.GroupJoined {
					break
				}
				// => GroupServing : if shards are serving
				for _, s := range argGroup.Shards {
					if s.Gid == localGroup.Id && s.Status != common.SERVING {
						break outer
					}
				}
				localGroup.Status = common.GroupServing
				sm.log.Infof("group %d in node %d status => GroupServing", localGroup.Id, node.Id)
			case common.GroupServing:
			case common.GroupLeaving:
				if argGroup.Status != common.GroupLeaving {
					break
				}
				if argGroup.ConfNum != localGroup.RemoteConfNum {
					break
				}
				// => GroupRemoving : if shards are invalid
				for _, s := range argGroup.Shards {
					if s.ExOwner == localGroup.Id && s.Status != common.INVALID {
						break outer
					}
				}
				localGroup.Status = common.GroupRemoving
				sm.log.Infof("group %d in node %d status => GroupRemoving", localGroup.Id, node.Id)
			case common.GroupRemoving:
				if argGroup.Status != common.GroupRemoving {
					break
				}
				var ngs []common.ConfigNodeGroup
				for i := len(sm.configs) - 2; i >= 0; i-- {
					conf := sm.configs[i]
					if ngs, ok = conf.Groups[gid]; ok {
						break
					}
				}
				if ngs == nil {
					log.Panicln("ngs is nil")
				}
				for _, ng := range ngs {
					if g, ok := sm.nodes[ng.NodeId].Groups[gid]; ok {
						if g != nil && g.Status != common.GroupRemoving && g.Status != common.GroupRemoved {
							break outer
						}
					}
				}
				localGroup.Status = common.GroupRemoved
				sm.log.Infof("group %d in node %d status => GroupRemoved", localGroup.Id, node.Id)

			case common.GroupRemoved:
				if argGroup.Status != common.GroupRemoved {
					sm.log.Warnf("master group %d in node %d is removed but replica group is %d", gid, node.Id, argGroup.Status)
				}

				if _, ok = node.Groups[gid]; ok {
					delete(node.Groups, gid)

					for _, n := range sm.nodes {
						if _, ok := n.Groups[gid]; ok {
							break outer
						}
					}
					newRouteConfig := common.ConfigV1{
						Num:    sm.routeConfig.Num + 1,
						Groups: sm.deepCopyConfMap(sm.routeConfig),
					}
					delete(newRouteConfig.Groups, gid)

					sm.routeConfig = newRouteConfig
					sm.log.Infof("group route config change to %v", newRouteConfig)
				}
			}

		}
	}

	// update node status
	node.Status = common.NodeNormal
	node.LastBeat = time.Now()

	// read config for each group
	for gid, group := range node.Groups {
		requestNum := group.ConfNum + 1
		if group.Status == common.GroupJoined {
			requestNum--
		}
		if requestNum >= 0 && requestNum < len(sm.configs) {
			reply.Configs[gid] = sm.configs[requestNum]
		} else {
			reply.Configs[gid] = sm.configs[len(sm.configs)-1]
		}
		prevConfNum := reply.Configs[gid].Num - 1
		if prevConfNum >= 0 {
			reply.PrevConfigs[gid] = sm.configs[prevConfNum]
		}
	}
	reply.LatestConf = sm.routeConfig

	// read nodes
	for nodeId, node := range sm.nodes {
		reply.Nodes[nodeId] = common.NodeInfo{
			Id: nodeId,
			Addr: node.Addr,
		}
	}

	// read groups
	for gid, g := range node.Groups {
		reply.Groups[gid] = *g
	}

	return
}

func (sm *ShardMaster) executeJoin(args *common.JoinArgs) (reply *common.JoinReply, err error) {

	latestConfig := sm.getLatestConfig()

	// copy the groups map
	groups := make(map[int][]common.ConfigNodeGroup)
	for gid, servers := range latestConfig.Groups {
		groups[gid] = make([]common.ConfigNodeGroup, len(servers))
		copy(groups[gid], servers)
	}

	// copy the shards
	shards := [common.NShards]int{}
	for i, v := range latestConfig.Shards {
		shards[i] = v
	}

	newRouteConfig := common.ConfigV1{
		Num: sm.routeConfig.Num + 1,
	}

	// copy the groups map
	newRouteConfig.Groups = sm.deepCopyConfMap(sm.routeConfig)

	// join
	for gid, nodes := range args.Nodes {
		cngs := make([]common.ConfigNodeGroup, 0)
		for i, nodeId := range nodes {
			if node, ok := sm.nodes[nodeId]; !ok {
				return &common.JoinReply{
					Err:         common.ErrNodeNotRegister,
					WrongLeader: false,
				}, nil
			} else {
				cngs = append(cngs, common.ConfigNodeGroup{
					NodeId: nodeId,
					Addr: node.Addr,
					RaftPeer: i,
				})
				node.Groups[gid] = &common.GroupInfo{
					Id:            gid,
					ConfNum:       latestConfig.Num + 1,
					Status:        common.GroupJoined,
					RemoteConfNum: latestConfig.Num + 1,
				}
			}
		}
		groups[gid] = cngs
		newRouteConfig.Groups[gid] = cngs
	}

	sm.configs = append(sm.configs, common.ConfigV1{
		Num:    latestConfig.Num + 1,
		Groups: groups,
		Shards: shards,
	})
	sm.routeConfig = newRouteConfig

	// re-balance
	sm.rebalanced()

	return &common.JoinReply{
		Err:         common.OK,
		WrongLeader: false,
	}, nil
}

func (sm *ShardMaster) executeLeave(args *common.LeaveArgs) (reply *common.LeaveReply, err error) {

	latestConfig := sm.getLatestConfig()

	leaveGidMap := make(map[int]bool)
	for _, gid := range args.GIDs {
		leaveGidMap[gid] = true
	}

	for gid, _ := range leaveGidMap {
		ngs := latestConfig.Groups[gid]
		for _, ng := range ngs {
			groupInfo := sm.nodes[ng.NodeId].Groups[gid]
			if groupInfo.Status != common.GroupServing {
				sm.log.Errorf("group %d in node %d is not serving, can't leave", gid, ng.NodeId)
				return &common.LeaveReply{
					Err:         common.ErrGroupNotServing,
					WrongLeader: false,
				}, nil
			}
		}
	}

	for gid, _ := range leaveGidMap {
		ngs := latestConfig.Groups[gid]
		for _, ng := range ngs {
			groupInfo := sm.nodes[ng.NodeId].Groups[gid]
			groupInfo.Status = common.GroupLeaving
			groupInfo.RemoteConfNum = latestConfig.Num + 1
			sm.log.Infof("group %d in node %d status => GroupLeaving", gid, ng.NodeId)

		}
	}

	// copy and remove
	groups := make(map[int][]common.ConfigNodeGroup)
	for gid, servers := range latestConfig.Groups {
		if _, ok := leaveGidMap[gid]; !ok {
			groups[gid] = servers
		}
	}
	shards := [common.NShards]int{}
	for i := 0; i < len(shards); i++ {
		if _, ok := leaveGidMap[latestConfig.Shards[i]]; !ok {
			shards[i] = latestConfig.Shards[i]
		} else {
			shards[i] = 0
		}
	}
	newConfig := common.ConfigV1{
		Num:    latestConfig.Num + 1,
		Groups: groups,
		Shards: shards,
	}
	sm.configs = append(sm.configs, newConfig)

	// rebalanced (shards of left groups should move to present groups)
	sm.rebalanced()

	return &common.LeaveReply{
		Err:         common.OK,
		WrongLeader: false,
	}, nil
}

func (sm *ShardMaster) executeMove(args *common.MoveArgs) (reply *common.MoveReply, err error) {

	latestConfig := sm.getLatestConfig()

	// copy the groups map
	groups := make(map[int][]common.ConfigNodeGroup)
	for gid, servers := range latestConfig.Groups {
		groups[gid] = servers
	}

	// copy the shards
	shards := [common.NShards]int{}
	for i, v := range latestConfig.Shards {
		shards[i] = v
	}

	// move
	fromGid := latestConfig.Shards[args.Shard]
	toGid := args.GID
	shards[args.Shard] = toGid

	newConfig := common.ConfigV1{
		Num:    latestConfig.Num + 1,
		Groups: groups,
		Shards: shards,
	}
	sm.configs = append(sm.configs, newConfig)

	sm.log.Infof("ShardMaster %d execute move: move shard %d from group %d to group %d", sm.me, args.Shard, fromGid, toGid)

	return &common.MoveReply{
		Err:         common.OK,
		WrongLeader: false,
	}, nil
}

func (sm *ShardMaster) executeQuery(args *common.QueryArgs) (reply *common.QueryReply, err error) {
	var config common.ConfigV1
	if args.Num == -1 || args.Num >= len(sm.configs) {
		config = sm.getLatestConfig()
	} else {
		config = sm.configs[args.Num]
	}
	return &common.QueryReply{
		Err:         common.OK,
		WrongLeader: false,
		Config:      config,
	}, nil
}

func (sm *ShardMaster) executeShow(args *common.ShowArgs) (reply *common.ShowReply, err error) {

	reply = new(common.ShowReply)
	reply.Nodes, reply.Groups, reply.Shards = []common.ShowNodeRes{},[]common.ShowGroupRes{},[]common.ShowShardRes{}
	reply.Err = common.OK

	if args.Nodes {
		nodeIds := make([]int, 0)
		if len(args.NodeIds) == 0 {
			for nodeId, _ := range sm.nodes {
				nodeIds = append(nodeIds, nodeId)
			}
		} else {
			nodeIds = args.NodeIds
		}

		for _, nodeId := range nodeIds {
			node, ok := sm.nodes[nodeId]
			if !ok {
				reply.Nodes = append(reply.Nodes, common.ShowNodeRes {
					Found: false,
					Id: nodeId,
				})
				continue
			}
			gids := make([]int, 0)
			isLeader := make(map[int]bool)
			for gid, g := range node.Groups {
				gids = append(gids, gid)
				isLeader[gid] = g.IsLeader
			}
			info := common.ShowNodeRes {
				Found: true,
				Id: nodeId,
				Addr: node.Addr,
				Groups: gids,
				IsLeader: isLeader,
				Status: node.Status.String(),
				MetricAddr: node.MetricAddr,
			}
			reply.Nodes = append(reply.Nodes, info)
		}

	}

	if args.Groups {
		gids := make([]int, 0)
		g2n := sm.createGroup2Nodes()
		if len(args.GIDs) == 0 {
			for gid, _ := range g2n {
				gids = append(gids, gid)
			}
		} else {
			gids = args.GIDs
		}
		sort.Ints(gids)

		// config := sm.getLatestConfig()
		for _, gid := range gids {
			nodeIds, ok := g2n[gid]
			if !ok {
				reply.Groups = append(reply.Groups, common.ShowGroupRes{
					Found: false,
					Id: gid,
				})
				continue
			}
			// ngs, ok := config.Groups[gid]
			res := common.ShowGroupRes{
				Found: true,
				Id: gid,
				ByNode: []common.ShowGroupInfoByNode{},
			}
			latestConfNode, latestConfNum := -1, 0
			for _, nodeId := range nodeIds {
				n, ok := sm.nodes[nodeId]
				if !ok {
					continue
				}
				g, ok := n.Groups[gid]
				if !ok {
					continue
				}
				res.ByNode = append(res.ByNode, common.ShowGroupInfoByNode{
					Id: 	 nodeId,
					Addr:   n.Addr,
					Peer:   g.Peer,
					ConfNum: g.ConfNum,
					Status: g.Status.String(),
					IsLeader: g.IsLeader,
					Size: g.Size,
				})
				if g.ConfNum > latestConfNum {
					latestConfNode = nodeId
				} else if g.ConfNum == latestConfNum && g.IsLeader {
					latestConfNode = nodeId
				}
			}
			if latestConfNode != -1 {
				g := sm.nodes[latestConfNode].Groups[gid]
				for _, shard := range g.Shards {
					if shard.Status == common.SERVING || shard.Status == common.PULLING || shard.Status == common.WAITING {
						res.ShardCnt++
					}
				}
			}
			reply.Groups = append(reply.Groups, res)
		}
	}

	if args.Shards {
		gids := args.GIDs
		conf := sm.getLatestConfig()
		for _, gid := range gids {
			var latestConfGroup *common.GroupInfo
			latestConfNum := 0
			ngs := conf.Groups[gid]
			for _, ng := range ngs {
				node := sm.nodes[ng.NodeId]
				group, ok := node.Groups[gid]
				if !ok {
					continue
				}
				if group.ConfNum > latestConfNum {
					latestConfGroup = group
					latestConfNum = group.ConfNum
				}
			}
			if latestConfGroup == nil {
				continue
			}
			shards := latestConfGroup.Shards
			for _, shard := range shards {
				if shard.Status != common.INVALID {
					reply.Shards = append(reply.Shards, common.ShowShardRes{
						Id: shard.Id,
						Gid: gid,
						Status: shard.Status,
						Size: shard.Size,
						Capacity: shard.Capacity,
						RangeStart: shard.RangeStart,
						RangeEnd: shard.RangeEnd,
					})
				}
			}
		}
	}

	return
}

func (sm *ShardMaster) rebalanced() {
	latestConfig := sm.configs[len(sm.configs)-1]
	sm.log.Infof("ShardMaster %d begin rebalanced,current config: %v", sm.me, latestConfig)

	if len(latestConfig.Groups) == 0 {
		sm.log.Infof("ShardMaster %d rebalanced: zero group, do nothing", sm.me)
		return
	} else if len(latestConfig.Groups) == 1 {
		// if only one group, assign all shards to it
		toGid := 0
		for gid, _ := range latestConfig.Groups {
			toGid = gid
			break
		}
		for i := 0; i < len(latestConfig.Shards); i++ {
			latestConfig.Shards[i] = toGid
		}
		sm.configs[len(sm.configs)-1] = latestConfig

		sm.log.Infof("ShardMaster %d rebalanced: only one group %d, assign all", sm.me ,toGid)
		return
	}

	numOfShards := make(map[int]int)
	for gid, _ := range latestConfig.Groups {
		numOfShards[gid] = 0
	}
	for _, gid := range latestConfig.Shards {
		numOfShards[gid]++
	}
	sm.log.Debugf("ShardMaster %d rebalanced: numOfShards: %v", sm.me ,numOfShards)

	gidsOrderByNumShards := make([]int,0)
	for gid, _ := range numOfShards {
		gidsOrderByNumShards = append(gidsOrderByNumShards, gid)
	}
	sort.Slice(gidsOrderByNumShards, func(i, j int) bool {
		return numOfShards[gidsOrderByNumShards[i]] < numOfShards[gidsOrderByNumShards[j]]
	})
	sm.log.Debugf("ShardMaster %d rebalanced: gidsOrderByNumShards: %v", sm.me, gidsOrderByNumShards)

	numGroups := len(latestConfig.Groups)
	avg := common.NShards / numGroups
	numPlusOne := common.NShards % numGroups

	for i := len(gidsOrderByNumShards) - 1; i >= 0; i-- {
		fromGid := gidsOrderByNumShards[i]
		if numOfShards[fromGid] <= avg {
			break
		}
		target := avg
		if numPlusOne > 0 && fromGid != 0 {
			target = avg + 1
			numPlusOne--
		} else {
			target = avg
		}
		delta := numOfShards[fromGid] - target
		for j, gid := range latestConfig.Shards {
			if delta <= 0 {
				break
			}
			if gid == fromGid {
				latestConfig.Shards[j] = 0
				delta--
			}
		}
		numOfShards[fromGid] = target
	}

	for i := 0; i < len(gidsOrderByNumShards); i++ {
		toGid := gidsOrderByNumShards[i]
		if numOfShards[toGid] >= avg {
			break
		}
		target := 0
		if numPlusOne > 0 {
			target = avg + 1
			numPlusOne--
		} else {
			target = avg
		}
		delta := target - numOfShards[toGid]
		for j, gid := range latestConfig.Shards {
			if delta <= 0 {
				break
			}
			if gid == 0 {
				latestConfig.Shards[j] = toGid
				delta--
			}
		}
		numOfShards[toGid] = target
	}

	sm.configs[len(sm.configs)-1] = latestConfig

	sm.log.Infof("ShardMaster %d rebalanced: finished: %v", sm.me, latestConfig)
}

func (sm *ShardMaster) nodeStatusUpdater() {
	for {
		select {
		case <-sm.KilledC:
			sm.log.Infof("ShardMaster %d has been killed, stop nodeStatusUpdater loop", sm.me)
			return
		case <-time.After(time.Second * 1):
			// if _, isLeader := sm.rf.GetState(); !isLeader {
			// 	continue
			// }
			sm.mu.Lock()
			for _, node := range sm.nodes {
				if node.Status == common.NodeNormal && time.Since(node.LastBeat) >= time.Second * 3 {
					node.Status = common.NodeDisconnect
					sm.log.Infof("Node %d is disconnected", node.Id)
				}
			}
			sm.mu.Unlock()
		}
	}
}

func (sm *ShardMaster) leaderBalancer() {
	for {
		select {
		case <-sm.KilledC:
			sm.log.Infof("ShardMaster %d has been killed, stop nodeStatusUpdater loop", sm.me)
			return
		case <-time.After(time.Second * 2):
			if _, isLeader := sm.rf.GetState(); !isLeader {
				continue
			}
			sm.mu.RLock()
			leaderCnt := map[int]int{}
			minCnt, maxCnt := 2<<31-1, -1
			for _, node := range sm.nodes {
				if node.Status != common.NodeNormal {
					continue
				}
				leaderCnt[node.Id] = 0
				for _, g := range node.Groups {
					if g.IsLeader {
						leaderCnt[node.Id]++
					}
				}
			}
			for _, cnt := range leaderCnt {
				if  cnt < minCnt {
					minCnt = cnt
				}
				if cnt > maxCnt {
					maxCnt = cnt
				}
			}
			if maxCnt - minCnt >= 2 {

				leaderGroup :=  map[int][]*common.GroupInfo{}
				for nodeId, cnt := range leaderCnt {
					if cnt == maxCnt {
						for _, g := range sm.nodes[nodeId].Groups {
							if g.IsLeader {
								if leaderGroup[nodeId] == nil {
									leaderGroup[nodeId] = make([]*common.GroupInfo, 0)
								}
								leaderGroup[nodeId] = append(leaderGroup[nodeId], g)
							}
						}
					}
				}
				config := sm.getLatestConfig()
				outer:
				for nodeId, gs := range leaderGroup {
					for _, g := range gs {
						ngs := config.Groups[g.Id]
						for _, ng := range ngs {
							if ng.NodeId == nodeId {
								continue
							}
							if leaderCnt[ng.NodeId] != minCnt {
								continue
							}
							// transfer to ng.NodeId
							sm.log.Infof("group %d can transfer leader to node %d peer %d", g.Id, ng.NodeId, ng.RaftPeer)
							args := raft.TransferLeaderArgs{
								RPCArgBase: &netw.RPCArgBase{
									Gid:  g.Id,
								},
								Gid:    g.Id,
								NodeId: ng.NodeId,
							}
							reply := raft.TransferLeaderReply{}
							if ok := sm.makeEndAndCall(sm.nodes[nodeId].Addr, nodeId, netw.ApiTransferLeader, &args, &reply); !ok {
								sm.log.Errorf("failed to call TransferLeader to node %d", nodeId)
							} else {
								sm.log.Infof("call TransferLeader success, reply: %s", reply.Err)
							}
							break outer
						}
					}
				}
			}

			sm.mu.RUnlock()
		}
	}
}

func (sm *ShardMaster) Kill() {
	if sm.rf != nil {
		sm.rf.Kill()
	}
	for i := 0; i < 4; i++ {
		sm.KilledC <- 1
	}
	atomic.StoreInt32(&sm.killed, 1)
	if sm.listener != nil {
		if err := sm.listener.Close(); err != nil {
			sm.log.Errorf("fail to close rpc listener: %v", err)
		}
	}
	if sm.rpcServ != nil {
		sm.rpcServ.Stop()
	}
}

func (sm *ShardMaster) Killed() bool {
	return atomic.LoadInt32(&sm.killed) == 1
}

func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func StartServer(conf etc.MasterConf) *ShardMaster {
	_ = utils.CheckAndMkdir(conf.Raft.DataDir)
	_ = utils.CheckAndMkdir(conf.Raft.WalDir)

	sm := new(ShardMaster)
	sm.me = conf.Serv.Me
	sm.log, _ = common.InitLogger(conf.Serv.LogLevel, fmt.Sprintf("Master%d", sm.me))

	if len(conf.Serv.Servers) < 2 {
		// sm.log.Fatalf("raft requires at least 2 peer to work, given: %d", len(conf.Serv.Servers))
		return nil
	}

	sm.configs = make([]common.ConfigV1, 1)
	sm.configs[0].Groups = map[int][]common.ConfigNodeGroup{}
	sm.routeConfig = sm.configs[0]

	sm.nodes = map[int]*Node{}

	sm.opApplied = make(map[int]chan interface{})
	sm.appliedCond = sync.NewCond(&sync.Mutex{})
	sm.ckMaxSeq = make(map[int64]int64)
	sm.KilledC = make(chan int, 5)

	sm.applyCh = make(chan raft.ApplyMsg)

	snapshotPath := fmt.Sprintf("%s/snapshot-%d", conf.Raft.DataDir, conf.Serv.Me)
	raftstatePath := fmt.Sprintf("%s/raftstate-%d", conf.Raft.DataDir, conf.Serv.Me)

	persister, err := raft.MakeDiskPersister(snapshotPath, raftstatePath)
	if err != nil {
		sm.log.Fatalf("failed to make disk persister: %v", err)
		return nil
	}
	sm.persister = persister

	snapshot := persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) == 0 {
		sm.log.Infof("empty snapshot to recover")
	} else {
		if err := sm.applySnapshot(snapshot); err != nil {
			sm.log.Fatalf("failed to recover snapshot: %v", err)
			return nil
		}
		sm.log.Infof("recover from snapshot, lastApplied: %d", sm.lastApplied)
	}

	logFileName := fmt.Sprintf(conf.Raft.WalDir + "/logfile%d", conf.Serv.Me)
	logFileCap := conf.Raft.WalCap

	sm.rf = raft.Make(sm.rpcFunc ,len(conf.Serv.Servers), conf.Serv.Me, persister, sm.applyCh, true,
		logFileName, logFileCap, conf.Raft.LogLevel)

	servers := make([]*netw.ClientEnd, len(conf.Serv.Servers))
	for i, addr := range conf.Serv.Servers {
		server := netw.MakeRPCEnd(fmt.Sprintf("Master%d", i),  addr)
		servers[i] = server
	}
	sm.servers = servers

	addr := conf.Serv.Servers[sm.me]
	host := addr[:strings.Index(addr, ":")]
	sm.metricAddr = fmt.Sprintf("%s:%d", host, 9200 + sm.me)

	go sm.applyer()
	go sm.checkpointer()
	go sm.nodeStatusUpdater()
	go sm.leaderBalancer()

	go func() {
		tick := time.Tick(time.Second)
		for range tick {
			opsProcessed.Inc()
		}
	}()
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(sm.metricAddr, nil)
	}()

	return sm
}
