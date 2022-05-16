package netw

import (
	"context"
	"sync"
	"time"

	rpcx_client "github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/log"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
	"github.com/smallnest/rpcx/share"

	"github.com/allen1211/mrkv/internal/netw/codec"
	"github.com/allen1211/mrkv/pkg/common"
)

func init() {

	log.SetDummyLogger()

	share.Codecs[protocol.SerializeType(5)] = &codec.MsgpCodec{}
}

var unreliablePercentage 	int

func SetUnreliable(percentage int) {
	unreliablePercentage = percentage
}

type RpcxServer struct {
	Name		string
	Addr		string

	serv		*server.Server
}

func MakeRpcxServer(name, addr string) *RpcxServer {
	s := server.NewServer()
	return &RpcxServer{
		Name: name,
		Addr: addr,
		serv: s,
	}
}

func (s *RpcxServer) Register(name string, obj interface{}) error {
	return s.serv.RegisterName(name, obj, "")
}

func (s *RpcxServer) Start() error {
	return s.serv.Serve("tcp", s.Addr)
}


func (s *RpcxServer) Stop() {
	_ = s.serv.Close()
}

type ClientEnd struct {
	sync.RWMutex
	Name		string
	Addr		string
	client 		rpcx_client.XClient

	tsr common.ThreadSafeRand
}

func MakeRPCEnd(name, addr string) *ClientEnd {
	ce := &ClientEnd{
		Name: name,
		Addr: addr,
		tsr:  common.MakeThreadSafeRand(time.Now().UnixNano()),
	}
	d, err := rpcx_client.NewPeer2PeerDiscovery("tcp@"+addr, "")
	if err != nil {
		return nil
	}
	option := rpcx_client.DefaultOption
	option.SerializeType = protocol.SerializeType(5)
	cli := rpcx_client.NewXClient(name, rpcx_client.Failfast, rpcx_client.RoundRobin, d, option)
	ce.client = cli

	return ce
}


func (ce *ClientEnd) Call(svrName string, args interface{}, reply interface{}) bool {
	if unreliablePercentage > 0 {
		if ce.tsr.Intn(100) <= unreliablePercentage {
			time.Sleep(1*time.Second)
			return false
		}
	}

	err := ce.client.Call(context.Background(), svrName, args, reply)
	if err != nil {
		// logrus.Errorf("call %s %s error: %v", ce.Name, svrName, err)
		return false
	}
	return true
}

func (ce*ClientEnd) Close() {
	if ce.client != nil {
		ce.client.Close()
	}
}