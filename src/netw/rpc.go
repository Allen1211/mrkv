package netw

import (
	"net"
	"net/rpc"
	"sync"
	"time"

	"mrkv/src/common"
)

func init()  {
	rpc.HandleHTTP()
}

var unreliablePercentage 	int

func SetUnreliable(percentage int) {
	unreliablePercentage = percentage
}

type ClientEnd struct {
	 sync.RWMutex
	 Name		string
	 Network	string
	 Addr		string
	 client 	*rpc.Client

	 tsr		common.ThreadSafeRand
}

func MakeRPCEnd(name, network, addr string) *ClientEnd {
	ce := &ClientEnd {
		Name: name,
		Addr: addr,
		Network: network,
		tsr: common.MakeThreadSafeRand(time.Now().UnixNano()),
	}
	return ce
}

func (ce *ClientEnd) Call(svrName string, args interface{}, reply interface{}) bool {
	if !ce.isConnected() {
		if err := ce.connect(0); err != nil {
			return false
		}
	}
	if unreliablePercentage > 0 {
		if ce.tsr.Intn(100) <= unreliablePercentage {
			time.Sleep(1*time.Second)
			return false
		}
	}

	err := ce.client.Call(svrName, args, reply)
	if err != nil {
		// log.Errorf("%v", err)
		ce.disconnect()
	}
	return err == nil
}


func (ce *ClientEnd) connect(maxRetry int) error {
	var conn net.Conn
	var err error
	var cli *rpc.Client
	for i := 0; i <= maxRetry; i++ {
		conn, err = net.DialTimeout(ce.Network, ce.Addr, 1*time.Second)
		if err != nil {
			continue
		}
		cli = rpc.NewClient(conn)
		ce.Lock()
		if ce.client == nil {
			ce.client = cli
		}
		ce.Unlock()
		return nil
	}
	return err
}

func (ce *ClientEnd) isConnected() bool {
	ce.RLock()
	connected := ce.client != nil
	ce.RUnlock()
	return connected
}

func (ce *ClientEnd) disconnect()  {
	ce.Lock()
	if ce.client != nil {
		_ = ce.client.Close()
		ce.client = nil
	}
	ce.Unlock()
}

func (ce* ClientEnd) Close() {
	if ce.client != nil {
		ce.client.Close()
	}
}