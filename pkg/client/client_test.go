package client

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"testing"
	"time"

	netw2 "github.com/allen1211/mrkv/internal/netw"
	common2 "github.com/allen1211/mrkv/pkg/common"
)

func Test1(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw2.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw2.MakeRPCEnd(fmt.Sprintf("Master%d", i),  addr)
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
	masters := make([]*netw2.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw2.MakeRPCEnd(fmt.Sprintf("Master%d", i), addr)
	}

	total, n := 1<<15, 16
	per := total / n

	begin := time.Now()

	val :=  randstring(100)
	var wg sync.WaitGroup
	for j := 0; j < n; j++ {
		wg.Add(1)
		go func(begin int) {
			ck := MakeUserClient(masters)
			for i := begin; i < begin + per; i++ {
				k, v := fmt.Sprintf("%d", i), val
				reply := ck.Put(k, []byte(v))
				if reply.Err != common2.OK {
					fmt.Println(reply.Err)
				}
			}
			wg.Done()
		}(j*per)
	}
	wg.Wait()

	cost := time.Since(begin)
	fmt.Println(cost.Seconds())

}

func Test3(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw2.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw2.MakeRPCEnd(fmt.Sprintf("Master%d", i),  addr)
	}
	total, n := 1<<15, 16
	per := total / n

	begin := time.Now()
	var wg sync.WaitGroup
	for j := 0; j < n; j++ {
		wg.Add(1)
		go func(begin int) {
			ck := MakeUserClient(masters)
			for i := begin; i < begin + per; i++ {
				k := fmt.Sprintf("%d", i)
				reply := ck.Get(k)
				if reply.Err != common2.OK {
					fmt.Println(reply.Err)
				}
			}
			wg.Done()
		}(j*per)
	}
	wg.Wait()
	cost := time.Since(begin)
	fmt.Println(cost.Seconds())

}

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}


func TestNoStop(t *testing.T) {
	servers := []string{":8000", ":8001", ":8002"}
	masters := make([]*netw2.ClientEnd, len(servers))
	for i, addr := range servers {
		masters[i] =  netw2.MakeRPCEnd(fmt.Sprintf("Master%d", i),  addr)
	}

	total, n := 1<<15, 8
	per := total / n

	val :=  randstring(100)
	var wg sync.WaitGroup
	for j := 0; j < n; j++ {
		wg.Add(1)
		go func(begin int) {
			ck := MakeUserClient(masters)
			for {
				for i := begin; i < begin + per; i++ {
					k, v := fmt.Sprintf("%d", i), val
					reply := ck.Put(k, []byte(v))
					if reply.Err != common2.OK {
						fmt.Println(reply.Err)
					}
				}
			}
		}(j*per)
	}
	wg.Wait()

}