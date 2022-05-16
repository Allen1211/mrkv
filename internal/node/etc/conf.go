package etc

import (
	"encoding/json"
	"io/ioutil"
	"runtime"

	log "github.com/sirupsen/logrus"
)

type NodeConf struct {
	NodeId	 int	`json:"node_id"`
	Host	 string `json:"host"`
	Port	 int	`json:"port"`
	Masters   	[]string `json:"masters"`
	DBPath		string	 `json:"db_dir"`

	Raft RaftConf
	Serv ServConf
}

type RaftConf struct {
	DataDir		string	 `json:"data_dir"`
	WalDir		string	 `json:"wal_dir"`
	WalCap		uint64	 `json:"wal_cap"`
	LogLevel 	string   `json:"log_level"`
}

type ServConf struct {
	Servers		[]string `json:"servers"`
	LogLevel 	string   `json:"log_level"`
	LogDir      string   `json:"log_dir"`
}

func MakeDefaultConfig() NodeConf {
	var defaultConf NodeConf
	if runtime.GOOS == "linux" {
		defaultConf = NodeConf{
			Host: "127.0.0.1",
			Port: 8800,
			DBPath: "/data/mrkvnode/data",
			Raft: RaftConf{
				DataDir:  "/data/mrkvnode/data",
				WalDir:   "/data/mrkvnode/logs",
				WalCap:   1048576,
				LogLevel: "info",
			},
			Serv: ServConf{
				LogLevel: "info",
			},
		}
	} else {
		defaultConf = NodeConf{
			Host: "127.0.0.1",
			Port: 8800,
			DBPath: "E:/MyWorkPlace/Project/MultiRaftKV/data/node",
			Raft: RaftConf{
				DataDir:  "E:/MyWorkPlace/Project/MultiRaftKV/data/node",
				WalDir:   "E:/MyWorkPlace/Project/MultiRaftKV/logs/node",
				WalCap:   1048576,
				LogLevel: "info",
			},
			Serv: ServConf{
				LogLevel: "info",
			},
		}
	}
	return defaultConf
}

func ParseNodeConf(confPath string) NodeConf {

	confBytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatalf("failed to open config file: %v", err)
	}
	conf := MakeDefaultConfig()
	if err := json.Unmarshal(confBytes, &conf); err != nil {
		log.Fatalf("failed to parse config file: %v", err)
	}
	return conf
}