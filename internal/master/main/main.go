package main

import (
	"flag"
	log "github.com/sirupsen/logrus"

	"github.com/allen1211/mrkv/internal/master"
	"github.com/allen1211/mrkv/internal/master/etc"
	"github.com/allen1211/mrkv/pkg/common"
	"github.com/allen1211/mrkv/pkg/common/labgob"
)

func main() {
	registerStructure()

	conf := makeConfig()

	server := startServer(conf)

	// go func() {
	// 	err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", 9080 + conf.Serv.Me), nil)
	// 	log.Println(err)
	// }()

	<-server.KilledC
}

func registerStructure()  {
	labgob.Register(common.OpBase{})
	labgob.Register(common.OpJoinCmd{})
	labgob.Register(common.OpLeaveCmd{})
	labgob.Register(common.OpMoveCmd{})
	labgob.Register(common.OpQueryCmd{})
	labgob.Register(common.OpHeartbeatCmd{})
	labgob.Register(common.OpShowCmd{})
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