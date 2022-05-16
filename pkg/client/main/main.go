package main

import (
	"flag"
	"strings"

	"github.com/allen1211/mrkv/pkg/client"
	"github.com/allen1211/mrkv/pkg/client/etc"
)

func main() {
	conf := makeConfig()

	cc := client.MakeConsoleClient(conf, nil, nil)
	cc.Start()

}

func makeConfig() etc.ClientConf {
	var mastersArg string
	flag.StringVar(&mastersArg, "masters", "", "master server address")
	flag.Parse()

	masters := strings.Split(mastersArg, ",")

	return etc.ClientConf{
		Masters: masters,
		LogLevel: "info",
	}

}