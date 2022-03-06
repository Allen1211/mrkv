package main

import (
	"fmt"

	"mrkv/src/netw"
	"mrkv/src/replica"
)

func main() {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), "tcp", addr)
	}

	clerk := replica.MakeClerk(masters)

	clerk.Put("a", "1")
	clerk.Put("b", "2")
	clerk.Put("c", "3")

	fmt.Println(clerk.Get("a"))
	fmt.Println(clerk.Get("b"))
	fmt.Println(clerk.Get("c"))
}
