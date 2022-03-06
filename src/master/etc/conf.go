package etc

import (
	"encoding/json"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
)

type MasterConf struct {
	Raft	RaftConf
	Serv	ServConf
}

type RaftConf struct {
	Servers		[]string `json:"servers"`
	Me			int 	 `json:"me"`
	DataDir		string	 `json:"data_dir"`
	WalDir		string	 `json:"wal_dir"`
	WalCap		uint64	 `json:"wal_cap"`
	MaxState	int		 `json:"max_state"`
	LogLevel 	string   `json:"log_level"`
}

type ServConf struct {
	Servers		[]string `json:"servers"`
	Me			int 	 `json:"me"`
	LogLevel 	string   `json:"log_level"`
}

func ParseMasterConf(confPath string) MasterConf {

	confBytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatalf("failed to open config file: %v", err)
	}
	conf := MasterConf{}
	if err := json.Unmarshal(confBytes, &conf); err != nil {
		log.Fatalf("failed to parse config file: %v", err)
	}
	return conf
}