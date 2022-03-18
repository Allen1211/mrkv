package main

import (
	"flag"
	"strings"

	"mrkv/src/client"
	"mrkv/src/client/etc"
)

func main() {
	conf := makeConfig()

	cc := client.MakeConsoleClient(conf)
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