package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"

	"github.com/Allen1211/mrkv/internal/master"
	"github.com/Allen1211/mrkv/internal/master/etc"
	"github.com/Allen1211/mrkv/pkg/common"
	"github.com/Allen1211/mrkv/pkg/common/labgob"
)

var (
	opsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "mrkv",
		Name:      "cnt",
		Help:      "The total number of processed events",
	})
)

func main() {
	registerStructure()

	conf := makeConfig()

	server := startServer(conf)

	// go func() {
	// 	err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", 9080 + conf.Serv.Me), nil)
	// 	log.Println(err)
	// }()

	go func() {
		tick := time.Tick(time.Second)
		for range tick {
			opsProcessed.Inc()
		}
	}()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(fmt.Sprintf(":%d", 2112 + conf.Serv.Me), nil)
	}()

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