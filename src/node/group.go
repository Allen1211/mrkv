package node

import (
	"mrkv/src/master"
	"mrkv/src/replica"
)

type Group struct {
	Id     int
	Status master.GroupStatus
	Peer   int
	RaftPeers int
	Size   int64
	replica 	*replica.ShardKV
}

func MakeGroup(id, peer, raftPeers int, status master.GroupStatus, replica *replica.ShardKV) *Group {
	return &Group {
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

func (g *Group) GetGroupInfo() *master.GroupInfo {
	if g.replica == nil {
		return &master.GroupInfo {
			Id: g.Id,
			Status: g.Status,
		}
	} else {
		res := g.replica.GetGroupInfo()
		res.Status = g.Status
		return res
	}
}

func (g *Group) UpdateConfig(conf master.ConfigV1)  {
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
	g.Status = master.GroupRemoved

}