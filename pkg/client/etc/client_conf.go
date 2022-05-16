package etc

import (
	"encoding/json"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
)

type ClientConf struct {
	Masters		[]string `json:"masters"`
	LogLevel	string	 `json:"log_level"`
}

func ParseClientConf(confPath string) ClientConf {

	confBytes, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatalf("failed to open config file: %v", err)
	}
	conf := ClientConf{}
	if err := json.Unmarshal(confBytes, &conf); err != nil {
		log.Fatalf("failed to parse config file: %v", err)
	}
	return conf
}