package etc

import (
	"encoding/json"
	"io/ioutil"
	"runtime"

	log "github.com/sirupsen/logrus"
)

type MasterConf struct {
	Raft	RaftConf
	Serv	ServConf
}

type RaftConf struct {
	DataDir		string	 `json:"data_dir"`
	WalDir		string	 `json:"wal_dir"`
	WalCap		uint64	 `json:"wal_cap"`
	MaxState	int		 `json:"max_state"`
	LogLevel 	string   `json:"log_level"`
}

type ServConf struct {
	Servers		[]string `json:"servers"`
	Me			int		 `json:"me"`
	LogLevel 	string   `json:"log_level"`
	LogDir      string   `json:"log_dir"`
}

func MakeDefaultConfig() MasterConf {
	var defaultConf MasterConf
	if runtime.GOOS == "linux" {
		defaultConf = MasterConf {
			Raft: RaftConf{
				DataDir:  "/data/mrkvmaster/data",
				WalDir:   "/data/mrkvmaster/logs",
				WalCap:   1048576,
				LogLevel: "info",
			},
			Serv: ServConf{
				LogLevel: "info",
				LogDir: "/var/log/mrkvmaster",
			},
		}
	} else {
		defaultConf = MasterConf {
			Raft: RaftConf{
				DataDir:  "E:/MyWorkPlace/Project/MultiRaftKV/data/master",
				WalDir:   "E:/MyWorkPlace/Project/MultiRaftKV/logs/master",
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

func ParseMasterConf(confPath string) MasterConf {

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