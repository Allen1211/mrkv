package main

import (
	"flag"

	log "github.com/sirupsen/logrus"

	"mrkv/src/common/labgob"
	"mrkv/src/master"
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
	labgob.Register(master.OpHeartbeatCmd{})
	labgob.Register(master.OpShowCmd{})
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
	server := master.StartServer(conf)
	if err := server.StartRPCServer(); err != nil {
		log.Fatalf("Start Raft RPC Server Error: %v", err)
	}

	return server
}