package main

import (
	"fmt"

	"mrkv/src/master"
	"mrkv/src/netw"
)

func mai1n() {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), "tcp", addr)
	}

	clerk := master.MakeClerk(masters)

	clerk.Join(map[int][]string{
		100: {":8100", ":8101", ":8102"},
		200: {":8200", ":8201", ":8202"},
	})
	// clerk.Leave([]int{200})
}
