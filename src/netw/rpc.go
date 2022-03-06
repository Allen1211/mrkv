package netw

import (
	"net/rpc"
	"sync"

	log "github.com/sirupsen/logrus"
)

func init()  {
	rpc.HandleHTTP()
}

type ClientEnd struct {
	 sync.RWMutex
	 Name		string
	 Network	string
	 Addr		string
	 client 	*rpc.Client
}

func MakeRPCEnd(name, network, addr string) *ClientEnd {
	ce := &ClientEnd{
		Name: name,
		Addr: addr,
		Network: network,
	}
	return ce
}

func (ce *ClientEnd) Call(svrName string, args interface{}, reply interface{}) bool {
	if !ce.isConnected() {
		if err := ce.connect(0); err != nil {
			return false
		}
	}

	err := ce.client.Call(svrName, args, reply)
	if err != nil {
		log.Errorf("%v", err)
		ce.disconnect()
	}
	return err == nil
}

func (ce *ClientEnd) CallAsync(svrName string, args interface{}, reply interface{}) <-chan error {
	res := make(chan error)
	c := make(chan *rpc.Call)
	go func() {
		if !ce.isConnected() {
			if err := ce.connect(3); err != nil {
				res <- err
			}
		}

		res <- ce.client.Go(svrName, args, reply, c).Error
	}()
	return res
}

func (ce *ClientEnd) connect(maxRetry int) error {
	var err error
	var cli *rpc.Client
	for i := 0; i <= maxRetry; i++ {
		cli, err = rpc.DialHTTP(ce.Network, ce.Addr)
		if err == nil {
			ce.Lock()
			if ce.client == nil {
				ce.client = cli
			}
			ce.Unlock()
			return nil
		}
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