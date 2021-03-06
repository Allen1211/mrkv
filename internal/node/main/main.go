package main

import (
	"flag"
	"fmt"

	// "net/http/pprof"
	_ "net/http/pprof"

	log "github.com/sirupsen/logrus"

	"github.com/allen1211/mrkv/internal/netw"
	"github.com/allen1211/mrkv/internal/node"
	"github.com/allen1211/mrkv/internal/node/etc"
	"github.com/allen1211/mrkv/internal/raft"
	"github.com/allen1211/mrkv/internal/replica"
	"github.com/allen1211/mrkv/pkg/common/labgob"
)

func init() {
	// pprofHandler := http.NewServeMux()
	// pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	// server := &http.Server{
	// 	Addr:    ":8989",
	// 	Handler: pprofHandler,
	// }
	// go server.ListenAndServe()

	// go func() {
	//  err := http.ListenAndServe("0.0.0.0:9091", nil)
	//  log.Println(err)
	// }()
}

func main() {
	registerStructure()

	conf := makeConfig()
	server := StartServer(conf)
	<-server.KilledC
}

func registerStructure()  {
	labgob.Register(replica.Op{})
	labgob.Register(replica.CmdBase{})
	labgob.Register(replica.KVCmd{})
	labgob.Register(replica.ConfCmd{})
	labgob.Register(replica.InstallShardCmd{})
	labgob.Register(replica.EraseShardCmd{})
	labgob.Register(replica.SnapshotCmd{})
	labgob.Register(replica.StopWaitingShardCmd{})
	labgob.Register(replica.EmptyCmd{})
	labgob.Register(raft.InstallSnapshotMsg{})
}

func makeConfig() etc.NodeConf {
	var confPath string
	flag.StringVar(&confPath, "c", "", "config file path")
	flag.Parse()

	if confPath == "" {
		log.Fatalf("no config file path provided")
	}

	return etc.ParseNodeConf(confPath)
}


func StartServer(conf etc.NodeConf) *node.Node {

	masters := make([]*netw.ClientEnd, len(conf.Masters))
	for i, addr := range conf.Masters {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), addr)
	}

	n := node.MakeNode(conf, masters, conf.Serv.LogLevel)

	if err := n.StartRPCServer(); err != nil {
		log.Fatalf("Start Raft RPC Server Error: %v", err)
	}

	return n
}