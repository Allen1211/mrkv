package main

import (
	"flag"
	"fmt"

	log "github.com/sirupsen/logrus"

	"mrkv/src/common/labgob"
	"mrkv/src/netw"
	"mrkv/src/raft"
	"mrkv/src/replica"
	"mrkv/src/replica/etc"
)

func main() {
	registerStructure()

	conf := makeConfig()
	server := startServer(conf)

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
	labgob.Register(raft.EmptyCmd{})
}

func makeConfig() etc.ReplicaConf {
	var confPath string
	flag.StringVar(&confPath, "c", "", "config file path")
	flag.Parse()

	if confPath == "" {
		log.Fatalf("no config file path provided")
	}

	return etc.ParseReplicaConf(confPath)
}

func startServer(conf etc.ReplicaConf) *replica.ShardKV {
	raftPeers := make([]*netw.ClientEnd, len(conf.Raft.Servers))
	for i, addr := range conf.Raft.Servers {
		peer := netw.MakeRPCEnd(fmt.Sprintf("Raft%d", i), "tcp", addr)
		raftPeers[i] = peer
	}
	persiter := raft.MakeMemoryPersister()

	logFileName := fmt.Sprintf(conf.Raft.WalDir + "/%d/logfile%d", conf.Gid, conf.Raft.Me)
	logFileCap := conf.Raft.WalCap

	ch := make(chan raft.ApplyMsg)
	persiter.DeepCopyLogFile(logFileName)
	rf := raft.Make(raftPeers, conf.Raft.Me, persiter, ch, true, logFileName, logFileCap, conf.Raft.LogLevel)

	if err := rf.StartRPCServer(); err != nil {
		log.Fatalf("Start Raft RPC Server Error: %v" ,err)
	}

	masters := make([]*netw.ClientEnd, len(conf.Masters))
	for i, addr := range conf.Masters {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), "tcp", addr)
	}

	servers := make([]*netw.ClientEnd, len(conf.Serv.Servers))
	for i, addr := range conf.Serv.Servers {
		server := netw.MakeRPCEnd(fmt.Sprintf("Replica-%d-%d", conf.Gid, i), "tcp", addr)
		servers[i] = server
	}

	dbPath := conf.DBPath + fmt.Sprintf("/db-%d-%d", conf.Gid, conf.Serv.Me)

	server := replica.StartServer(rf, ch, servers, conf.Serv.Me, persiter, conf.Gid, masters, dbPath, conf.Serv.LogLevel)
	if err := server.StartRPCServer(); err != nil {
		log.Fatalf("Start Raft RPC Server Error: %v", err)
	}

	return server
}