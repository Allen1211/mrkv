package node

import (
	"github.com/Allen1211/mrkv/internal/replica"
	"github.com/Allen1211/mrkv/pkg/common"
)

type Group struct {
	Id        int
	Status    common.GroupStatus
	Peer      int
	RaftPeers int
	Size      int64
	replica   *replica.ShardKV
}

func MakeGroup(id, peer, raftPeers int, status common.GroupStatus, replica *replica.ShardKV) *Group {
	return &Group{
		Id: id,
		Peer: peer,
		RaftPeers: raftPeers,
		Status: status,
		replica: replica,
	}
}

func (g *Group) ConfNum() int {
	return g.replica.GetCurrConfig().Num
}

func (g *Group) GetGroupInfo() *common.GroupInfo {
	if g.replica == nil {
		return &common.GroupInfo{
			Id: g.Id,
			Status: g.Status,
		}
	} else {
		res := g.replica.GetGroupInfo()
		res.Status = g.Status
		return res
	}
}

func (g *Group) UpdateConfig(conf common.ConfigV1)  {
	if g.replica != nil {
		g.replica.UpdateConfig(conf)
	}
}

func (g *Group) Shutdown() {

	if g.replica != nil {
		g.replica.Kill()
		g.replica.Clear()
		g.replica = nil
	}
	g.Status = common.GroupRemoved

}