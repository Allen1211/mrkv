package client

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"mrkv/src/netw"
)

func Test1(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), "tcp", addr)
	}
	ck := MakeUserClient(masters)

	mp := map[string]string{}
	for j := 0; j < 20; j++ {
		for i := 0; i < 50; i++ {
			k, v := fmt.Sprintf("%d", i * j), randstring(10)
			mp[k] = v
			ck.Put(k, []byte(v))
			fmt.Println("success")
			time.Sleep(50*time.Millisecond)
		}
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("%d", i * j)
			res := ck.Get(k)
			v1 := res.Value
			v2 := mp[k]
			if  string(v1) != v2 {
				t.Fatalf("get %s val %s (got) != %s (expected)", k, v1, v2)
			}
			fmt.Println("success")
			time.Sleep(50*time.Millisecond)

		}
	}
}

func Test2(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), "tcp", addr)
	}
	ck := MakeUserClient(masters)

	for i := 0; i < 1000; i++ {
		k, v := fmt.Sprintf("%d", i), randstring(100)
		ck.Put(k, []byte(v))
		fmt.Println("success")
	}
}

func Test3(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw.MakeRPCEnd(fmt.Sprintf("Master%d", i), "tcp", addr)
	}
	ck := MakeUserClient(masters)

	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("%d", i)
		reply := ck.Get(k)
		fmt.Println(reply.Err)
	}
}

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}
