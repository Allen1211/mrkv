package main

import (
	"flag"
	"fmt"

	log "github.com/sirupsen/logrus"

	"mrkv/src/common/labgob"
	"mrkv/src/master"
	"mrkv/src/netw"
	"mrkv/src/raft"

	"mrkv/src/master/etc"
)

func main() {
	registerStructure()

	conf := makeConfig()

	server := startServer(conf)

	<-server.KilledC
}

func registerStructure()  {
	labgob.Register(master.OpBase{})
	labgob.Register(master.OpJoinCmd{})
	labgob.Register(master.OpLeaveCmd{})
	labgob.Register(master.OpMoveCmd{})
	labgob.Register(master.OpQueryCmd{})
	labgob.Register(raft.EmptyCmd{})
}

func makeConfig() etc.MasterConf {
	var confPath string
	flag.StringVar(&confPath, "c", "", "config file path")
	flag.Parse()

	if confPath == "" {
		log.Fatalf("no config file path provided")
	}

	return etc.ParseMasterConf(confPath)
}

func startServer(conf etc.MasterConf) *master.ShardMaster {
	peers := make([]*netw.ClientEnd, len(conf.Raft.Servers))
	for i, addr := range conf.Raft.Servers {
		peer := netw.MakeRPCEnd(fmt.Sprintf("Raft%d", i), "tcp", addr)
		peers[i] = peer
	}
	persister := raft.MakeMemoryPersister()
	ch := make(chan raft.ApplyMsg)

	logFileName := fmt.Sprintf(conf.Raft.WalDir + "/logfile%d", conf.Raft.Me)
	logFileCap := conf.Raft.WalCap
	rf := raft.Make(peers, conf.Raft.Me, persister, ch, true, logFileName, logFileCap, conf.Raft.LogLevel)

	if err := rf.StartRPCServer(); err != nil {
		log.Fatalf("Start Raft RPC Server Error: %v" ,err)
	}

	servers := make([]*netw.ClientEnd, len(conf.Serv.Servers))
	for i, addr := range conf.Serv.Servers {
		server := netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), "tcp", addr)
		servers[i] = server
	}

	server := master.StartServer(servers, conf.Raft.Me, rf, ch, conf.Serv.LogLevel)
	if err := server.StartRPCServer(); err != nil {
		log.Fatalf("Start Raft RPC Server Error: %v", err)
	}

	return server
}